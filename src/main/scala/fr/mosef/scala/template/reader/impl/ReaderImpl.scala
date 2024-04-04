package fr.mosef.scala.template.reader.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.reader.Reader
import java.util.Properties
import java.io.FileInputStream

class ReaderCSV(sparkSession: SparkSession, propertiesFilePath: String) extends Reader {
  val properties: Properties = new Properties()
  properties.load(new FileInputStream(propertiesFilePath))

  def read(path: String): DataFrame = {
    sparkSession
      .read
      .option("sep", properties.getProperty("read_separator"))
      .option("inferSchema", properties.getProperty("schema"))
      .option("header", properties.getProperty("read_header"))
      .format(properties.getProperty("read_format_csv"))
      .load(path)
  }

}


class ReaderParquet(sparkSession: SparkSession, propertiesFilePath: String) extends Reader {
  val properties: Properties = new Properties()
  properties.load(new FileInputStream(propertiesFilePath))

  def read(path: String): DataFrame = {
    sparkSession
      .read
      .format(properties.getProperty("read_format_parquet"))
      .load(path)
  }

}

class ReaderHive(sparkSession: SparkSession, propertiesFilePath: String) extends Reader {
  val properties: Properties = new Properties()
  properties.load(new FileInputStream(propertiesFilePath))
  private def createExternalTable(tableName: String, path: String, fileFormat: String): Unit = {
    sparkSession.sql(s"""
                        |CREATE EXTERNAL TABLE IF NOT EXISTS $tableName
                        |STORED AS $fileFormat
                        |LOCATION '$path'
                      """.stripMargin)
  }
  def read(path: String): DataFrame = {
    createExternalTable(properties.getProperty("table_name"), path, properties.getProperty("reader_format_hive"))
    sparkSession.table(properties.getProperty("table_name"))
  }
}


