package org.apache.spark.sql.fdb

import java.util.{Optional, UUID}

import com.apple.foundationdb.Range
import com.guandata.spark.fdb.{ColumnDataType, FdbBufferedDeleter, FdbBufferedReader, FdbBufferedWriter, FdbException, FdbInstance, FdbStorage}
import org.apache.spark.sql.{Row, RowFactory, SaveMode}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._


class FdbDataReader(reader: FdbBufferedReader) extends DataReader[Row] {
  override def next(): Boolean = reader.next()

  override def get(): Row = {
    // Spark don't support UUID, we need to cast UUID to String
    val cellValues = reader.get().map{
      case cell: UUID if cell != null =>
        cell.toString.replaceAllLiterally("-", "")
      case cell =>
        cell
    }
    RowFactory.create(cellValues: _*)
  }

  override def close(): Unit = ()
}


class FdbDataReaderFactory(domainId: String, tableName: String, locations: Seq[String], begin: Array[Byte], end: Array[Byte]) extends DataReaderFactory[Row] {
  override def preferredLocations: Array[String] = {
    locations.toArray
  }

  override def createDataReader(): DataReader[Row] = {
    val storage = new FdbStorage(domainId)
    val tableDefinition = storage.getTableDefinition(tableName).get
    new FdbDataReader(new FdbBufferedReader(tableDefinition, storage, new Range(begin, end)))
  }
}

class FdbDataSourceReader(domainId: String, tableName: String) extends DataSourceReader {
  private val storage = new FdbStorage(domainId)
  private val tableDefinition = storage.getTableDefinition(tableName).get
  override def readSchema(): StructType = {
    FdbUtil.convertTableDefinitionToStructType(tableDefinition)
  }

  override def createDataReaderFactories(): java.util.List[DataReaderFactory[Row]] = {
    val localityInfos = storage.getLocalityInfo(tableName)
    localityInfos.map{ case (locations, range) =>
      new FdbDataReaderFactory(domainId = domainId,
        tableName = tableName,
        locations = locations,
        begin = range.begin,
        end = range.end).asInstanceOf[DataReaderFactory[Row]]
    }.asJava
  }
}

case class FdbWriterCommitMessage(message: String) extends WriterCommitMessage

class FdbDataWriter(domainId: String, tableName: String, isDeleteRows: Boolean) extends DataWriter[Row] {
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

  override def write(record: Row): Unit = {
    if (insertColumnNames.isEmpty) {
      insertColumnNames = if (isDeleteRows) {
        Option(tableDefinition.primaryKeys.toVector)
      } else {
        Option(record.schema.map{_.name}.toVector)
      }
    }
    val cellValues = scala.Range(0, record.length).map{ i =>
      record.get(i).asInstanceOf[AnyRef]
    }
    writer.insertRow(insertColumnNames.get, cellValues)
  }
}

class FdbDataWriterFactory(domainId: String, tableName: String, isDeleteRows: Boolean) extends DataWriterFactory[Row] {
  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    new FdbDataWriter(domainId = domainId, tableName = tableName, isDeleteRows = isDeleteRows)
  }
}

class FdbDataSourceWriter(domainId: String, tableName: String, isDeleteRows: Boolean) extends DataSourceWriter {
  override def commit(messages: Array[WriterCommitMessage]): Unit = {
  }

  override def createWriterFactory(): DataWriterFactory[Row] = {
    new FdbDataWriterFactory(domainId = domainId, tableName = tableName, isDeleteRows = isDeleteRows)
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
        Optional.of(new FdbDataSourceWriter(domainId = domainId, tableName = tableName, isDeleteRows = true))
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
          val existingTableStruct = FdbUtil.convertTableDefinitionToStructType(tableDefinitionOpt.get).filterNot(_.name == FdbInstance.sysIdColumnName)
          val toInsertStruct = schema.filterNot(_.name == FdbInstance.sysIdColumnName)
          if (existingTableStruct != toInsertStruct) {
            throw new FdbException("Insert Data don't compatible with existing table")
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

        Optional.of(new FdbDataSourceWriter(domainId = domainId, tableName = tableName, isDeleteRows = false))
      }
    }
  }
}
