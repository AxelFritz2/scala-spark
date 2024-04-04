package fr.mosef.scala.template.reader.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.reader.Reader
import java.util.Properties
import java.io.FileInputStream

class ReaderCSV(sparkSession: SparkSession, propertiesFilePath: String) extends Reader {
  val properties: Properties = new Properties()
  properties.load(new FileInputStream(propertiesFilePath))

  def read(format: String, options: Map[String, String], path: String): DataFrame = {
    sparkSession
      .read
      .options(options)
      .format(format)
      .load(path)
  }

  def read(path: String): DataFrame = {
    sparkSession
      .read
      .option("sep", properties.getProperty("read_separator"))
      .option("inferSchema", properties.getProperty("schema"))
      .option("header", properties.getProperty("read_header"))
      .format(properties.getProperty("read_format_csv"))
      .load(path)
  }

  def read(): DataFrame = {
    sparkSession.sql("SELECT 'Empty DataFrame for unit testing implementation")
  }
}


class ReaderParquet(sparkSession: SparkSession, propertiesFilePath: String) extends Reader {
  val properties: Properties = new Properties()
  properties.load(new FileInputStream(propertiesFilePath))

  def read(format: String, options: Map[String, String], path: String): DataFrame = {
    sparkSession
      .read
      .options(options)
      .format(format)
      .load(path)
  }

  def read(path: String): DataFrame = {
    sparkSession
      .read
      .format(properties.getProperty("read_format_parquet"))
      .load(path)
  }

  def read(): DataFrame = {
    sparkSession.sql("SELECT 'Empty DataFrame for unit testing implementation")
  }
}
