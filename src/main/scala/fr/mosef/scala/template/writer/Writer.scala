package fr.mosef.scala.template.writer

import org.apache.spark.sql.DataFrame

trait Writer {
  def write(df: DataFrame, mode: String = "overwrite", path: String): Unit
}
