package fr.mosef.scala.template.processor

import org.apache.spark.sql.DataFrame

trait Processor {

  def groupby(inputDF: DataFrame) : DataFrame
  def sum(inputDF: DataFrame) : DataFrame
  def mean(inputDF: DataFrame) : DataFrame

}
