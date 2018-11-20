package org.apache.spark.sql.fdb

import java.util.{Optional, UUID}

import com.apple.foundationdb.Range
import com.guandata.spark.fdb.{BaseBufferedReader, ColumnDataType, FdbBufferedDeleter, FdbBufferedWriter, FdbException, FdbInstance, FdbStorage}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{RowFactory, SaveMode}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._


class FdbInputPartitionReader(schema: StructType, reader: BaseBufferedReader) extends InputPartitionReader[InternalRow] {
  private val rowEncoder = RowEncoder.apply(schema).resolveAndBind()

  override def next(): Boolean = reader.next()

  override def get(): InternalRow = {
    // Spark don't support UUID, we need to cast UUID to String
    val cellValues = reader.get().map{
      case cell: UUID if cell != null =>
        cell.toString.replaceAllLiterally("-", "")
      case cell =>
        cell
    }
    rowEncoder.toRow(RowFactory.create(cellValues: _*))
  }

  override def close(): Unit = {
    reader.close()
  }
}


class FdbInputPartitionReaderFactory(schema: StructType, domainId: String, tableName: String, locations: Seq[String], begin: Array[Byte], end: Array[Byte]) extends InputPartition[InternalRow] {
  override def preferredLocations: Array[String] = {
    locations.toArray
  }

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    val storage = new FdbStorage(domainId)
    val tableDefinition = storage.getTableDefinition(tableName).get
    new FdbInputPartitionReader(schema, BaseBufferedReader.createBufferedReader(tableDefinition, storage, new Range(begin, end)))
  }
}

class FdbDataSourceReader(domainId: String, tableName: String) extends DataSourceReader {
  private val storage = new FdbStorage(domainId)
  private val tableDefinition = storage.getTableDefinition(tableName).get
  override def readSchema(): StructType = {
    FdbUtil.convertTableDefinitionToStructType(tableDefinition)
  }

  override def planInputPartitions(): java.util.List[InputPartition[InternalRow]] = {
    if (storage.isRocksDB) {
      // we don't support read localityInfo for RocksDB currently
      val range = storage.openDataDir(tableName).range()
      List(new FdbInputPartitionReaderFactory(schema = readSchema(),
        domainId = domainId,
        tableName = tableName,
        locations = Seq.empty[String],
        begin = range.begin,
        end = range.end).asInstanceOf[InputPartition[InternalRow]]
      ).asJava
    } else {
      val localityInfos = storage.getLocalityInfo(tableName)
      localityInfos.map { case (locations, range) =>
        new FdbInputPartitionReaderFactory(schema = readSchema(),
          domainId = domainId,
          tableName = tableName,
          locations = locations,
          begin = range.begin,
          end = range.end).asInstanceOf[InputPartition[InternalRow]]
      }.asJava
    }
  }
}

case class FdbWriterCommitMessage(message: String) extends WriterCommitMessage

class FdbDataWriter(schema: StructType, domainId: String, tableName: String, isDeleteRows: Boolean) extends DataWriter[InternalRow] {
  private val rowEncoder = RowEncoder.apply(schema).resolveAndBind()
  private val tableDefinition = new FdbStorage(domainId).getTableDefinition(tableName).get
  private val writer = {
    if (isDeleteRows) {
      new FdbBufferedDeleter(domainId = domainId, tableDefinition = tableDefinition)
    } else {
      new FdbBufferedWriter(domainId = domainId, tableDefinition = tableDefinition, enableMerge = false)
    }
  }

  private var insertColumnNames: Option[Vector[String]] = None

  override def commit(): WriterCommitMessage = {
    writer.flush()
    FdbWriterCommitMessage("success")
  }

  override def abort(): Unit = {

  }

  override def write(row: InternalRow): Unit = {
    if (insertColumnNames.isEmpty) {
      insertColumnNames = if (isDeleteRows) {
        Option(tableDefinition.primaryKeys.toVector)
      } else {
        Option(schema.map{_.name}.toVector)
      }
    }
    val record = rowEncoder.fromRow(row)
    val cellValues = scala.Range(0, record.length).map{ i =>
      record.get(i).asInstanceOf[AnyRef]
    }
    writer.insertRow(insertColumnNames.get, cellValues)
  }
}

class FdbDataWriterFactory(schema: StructType, domainId: String, tableName: String, isDeleteRows: Boolean) extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    new FdbDataWriter(schema = schema, domainId = domainId, tableName = tableName, isDeleteRows = isDeleteRows)
  }
}

class FdbDataSourceWriter(schema: StructType, domainId: String, tableName: String, isDeleteRows: Boolean) extends DataSourceWriter {
  override def commit(messages: Array[WriterCommitMessage]): Unit = {
  }

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new FdbDataWriterFactory(schema = schema, domainId = domainId, tableName = tableName, isDeleteRows = isDeleteRows)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
  }
}

class DefaultSource extends DataSourceV2 with ReadSupport with WriteSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val domainId = options.get("domain").get()
    val tableName = options.get("table").get()
    new FdbDataSourceReader(domainId = domainId, tableName = tableName)
  }

  override def createWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {
    val domainId = options.get("domain").get()
    val tableName = options.get("table").get()
    val storage = new FdbStorage(domainId)
    val tableDefinitionOpt = storage.getTableDefinition(tableName).toOption

    val isDeleteRows = options.getBoolean("isDeleteRows", false)
    if (isDeleteRows) {
      if (tableDefinitionOpt.isEmpty) {
        throw new FdbException("Table not exists!")
      } else if (tableDefinitionOpt.get.primaryKeys.size != schema.fields.length) {
        throw new FdbException("Not all primary keys are provided, to DELETE rows, the source DataSet should and only contains all primary key values!")
      } else {
        Optional.of(new FdbDataSourceWriter(schema = schema, domainId = domainId, tableName = tableName, isDeleteRows = true))
      }
    } else {
      var skip = false
      var checkSchemaCompatible = false
      var clearCurrentData = false
      var createTable = false
      if (tableDefinitionOpt.nonEmpty) {
        mode match {
          case SaveMode.Append =>
            checkSchemaCompatible = true
          case SaveMode.ErrorIfExists =>
            throw new FdbException("Table already exists!")
          case SaveMode.Ignore =>
            skip = true
          case SaveMode.Overwrite =>
            checkSchemaCompatible = true
            clearCurrentData = true
        }
      } else {
        createTable = true
      }

      if (skip) {
        Optional.empty[DataSourceWriter]()
      } else {
        if (checkSchemaCompatible) {
          val existingTableStruct = StructType(FdbUtil.convertTableDefinitionToStructType(tableDefinitionOpt.get).fields.filterNot(_.name == FdbInstance.sysIdColumnName))
          val toInsertStruct = StructType(schema.fields.filterNot(_.name == FdbInstance.sysIdColumnName))
          if (!FdbUtil.checkTwoTableDefinitionContainSameColumns(existingTableStruct, toInsertStruct)) {
            throw new FdbException(s"Insert Data don't compatible with existing table, existing: ${existingTableStruct.toString}, toInsert: ${toInsertStruct.toString}")
          }
        }

        if (createTable) {
          val columnNameTypes = schema.map { field =>
            if (field.name == FdbInstance.sysIdColumnName) {
              field.name -> ColumnDataType.UUIDType
            } else {
              field.name -> ColumnDataType.from(field.dataType.toString)
            }
          }
          storage.createTable(tableName, columnNameTypes, Option(columnNameTypes.exists(_._1 == FdbInstance.sysIdColumnName)).collect { case true => FdbInstance.sysIdColumnName }.toSeq)
        }

        if (clearCurrentData) {
          storage.truncateTable(tableName)
        }

        Optional.of(new FdbDataSourceWriter(schema = schema, domainId = domainId, tableName = tableName, isDeleteRows = false))
      }
    }
  }
}
