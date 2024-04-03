package fr.mosef.scala.template.writer.impl


import fr.mosef.scala.template.writer.Writer
import org.apache.spark.sql.DataFrame


class WriterCSV() extends Writer {

  def write(df: DataFrame, mode: String = "overwrite", path: String): Unit = {
    df
      .write
      .option("header", "true")
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