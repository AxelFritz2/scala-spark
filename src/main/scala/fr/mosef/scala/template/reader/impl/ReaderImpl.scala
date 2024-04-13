package fr.mosef.scala.template.reader.impl

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import fr.mosef.scala.template.reader.Reader

import java.util.Properties
import java.io.FileInputStream

class ReaderCSV(sparkSession: SparkSession, propertiesFilePath: String) extends Reader {
  val properties: Properties = new Properties()
  properties.load(new FileInputStream(propertiesFilePath))

  println("************************************************************************************************")
  println("Reader CSV")
  println("************************************************************************************************")
  println(properties)
  println("************************************************************************************************")

  def read(path: String): DataFrame = {
    val df = sparkSession
      .read
      .option("sep", properties.getProperty("read_separator"))
      .option("inferSchema", properties.getProperty("schema"))
      .option("header", properties.getProperty("read_header"))
      .format(properties.getProperty("read_format_csv"))
      .load(path)

    val header = properties.getProperty("read_header").toBoolean

    if (!header) {
      val firstRow: Row = df.first()
      val columnNames: Seq[String] = firstRow.toSeq.map(_.toString)
      val data = df.filter(row => row != firstRow).toDF(columnNames: _*)
      data
    } else {
      df
    }
  }
}


class ReaderParquet(sparkSession: SparkSession, propertiesFilePath: String) extends Reader {
  val properties: Properties = new Properties()
  properties.load(new FileInputStream(propertiesFilePath))

  println("************************************************************************************************")
  println("Reader Parquet")
  println("************************************************************************************************")
  println(properties)
  println("************************************************************************************************")

  def read(path: String): DataFrame = {
    sparkSession
      .read
      .format(properties.getProperty("read_format_parquet"))
      .load(path)
  }

}

class ReaderHive(sparkSession: SparkSession, propertiesFilePath: String, fileFormat: String) extends Reader {
  val properties: Properties = new Properties()
  properties.load(new FileInputStream(propertiesFilePath))

  println("************************************************************************************************")
  println("Reader Hive")
  println("************************************************************************************************")
  println(properties)
  println("************************************************************************************************")


  private def createInternalTable(tableName: String, schema: StructType, fileFormat: String): Unit = {
    val storedFormat = fileFormat match {
      case "parquet" => "PARQUET"
      case "csv" => "TEXTFILE"
      case _ => throw new IllegalArgumentException(s"File format $fileFormat not supported.")
    }

    sparkSession.sql(s"""
      CREATE TABLE IF NOT EXISTS $tableName
      (${schema.fields.map(f => s"${f.name} ${f.dataType.simpleString}").mkString(", ")})
      STORED AS $storedFormat
    """)
  }

  private def loadDataIntoTable(tableName: String, path: String, fileFormat: String, schema: StructType): Unit = {
    val df = fileFormat match {
      case "parquet" =>
        sparkSession.read.format("PARQUET").load(path)
      case "csv" =>
        sparkSession.read.option("header", "true").schema(schema).csv(path)
      case _ => throw new IllegalArgumentException(s"File format $fileFormat not supported.")
    }

    df.write.mode(SaveMode.Overwrite).insertInto(tableName)
  }

  def read(path: String): DataFrame = {
    val tableName = properties.getProperty("table_name")

    val schema = fileFormat match {
      case "csv" =>
        val inferredSchema = sparkSession.read.option("header", "true").option("inferSchema", "true").csv(path).schema
        val customSchema = StructType(inferredSchema.map { field =>
          if (field.name == "ndeg_de_version" || field.name == "numero_de_contact") {
            StructField(field.name, LongType)
          } else {
            field
          }
        })
        customSchema
      case "parquet" =>
        val inferredSchema = sparkSession.read.format(fileFormat).load(path).schema
        val customSchema = StructType(inferredSchema.map { field =>
          if (field.name == "ndeg_de_version" || field.name == "numero_de_contact") {
            StructField(field.name, LongType)
          } else {
            field
          }
        })
        customSchema
    }

    if (fileFormat == "csv" || fileFormat == "parquet") {
      createInternalTable(tableName, schema, fileFormat)
      loadDataIntoTable(tableName, path, fileFormat, schema)
    }

    sparkSession.table(tableName)
  }
}



