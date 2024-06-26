package fr.mosef.scala.template.writer.impl


import fr.mosef.scala.template.writer.Writer
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.FileInputStream
import java.util.Properties


class WriterCSV(propertiesFilePath: String) extends Writer {
  val properties: Properties = new Properties()
  properties.load(new FileInputStream(propertiesFilePath))
  def write(df: DataFrame, mode: String = "overwrite", path: String): Unit = {
    df
      .write
      .option("header", properties.getProperty("write_header"))
      .mode(mode)
      .csv(path)
  }
}

class WriterParquet() extends Writer {

  def write(df: DataFrame, mode: String = "overwrite", path: String): Unit = {
    df
      .write
      .mode(mode)
      .parquet(path)
  }
}

class WriterHive(sparkSession: SparkSession)  {
  def write(df: DataFrame, tableName: String, mode: String = "overwrite"): Unit = {
    df.write.mode(mode).saveAsTable(tableName)

    println(s"Contenu de la table $tableName :")
    sparkSession.table(tableName).show()
}}