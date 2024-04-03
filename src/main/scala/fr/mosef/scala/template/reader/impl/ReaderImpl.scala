package fr.mosef.scala.template.reader.impl

import fr.mosef.scala.template.PropertiesReader.ConfigLoader
import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.reader.Reader

class ReaderCSV(sparkSession: SparkSession) extends Reader {

  //confloader.loadProperties()
  //val format: String = ConfigLoader.getProperty("format").getOrElse("csv")
  //val separator: String = ConfigLoader.getProperty("separator").getOrElse(";")
  //val header: String = ConfigLoader.getProperty("header").getOrElse("true")


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
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .format("csv")
      .load(path)
  }

  def read(): DataFrame = {
    sparkSession.sql("SELECT 'Empty DataFrame for unit testing implementation")
  }
}

class ReaderParquet(sparkSession: SparkSession) extends Reader {
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
      .format("parquet")
      .load(path)
  }

  def read(): DataFrame = {
    sparkSession.sql("SELECT 'Empty DataFrame for unit testing implementation")
  }
}
