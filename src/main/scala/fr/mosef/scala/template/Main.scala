package fr.mosef.scala.template

import fr.mosef.scala.template.PropertiesReader.ConfigLoader
import fr.mosef.scala.template.job.Job
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.ReaderCSV
import fr.mosef.scala.template.reader.impl.ReaderParquet
import fr.mosef.scala.template.writer.Writer
import org.apache.spark.sql.SparkSession
import fr.mosef.scala.template.writer.impl.{WriterCSV, WriterParquet}

object Main extends App with Job {

  val cliArgs = args
  // Arguments d'entrée
  val MASTER_URL: String = try {
    cliArgs(0)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => "local[1]"
  }

  val SRC_PATH: String = try {
    cliArgs(1)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      print("No input defined")
      sys.exit(1)
    }
  }
  val DST_PATH: String = try {
    cliArgs(2)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./default/output-writer"
    }
  }

  val group_var: String = try {
    cliArgs(3)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "group_key"
    }
  }

  val op_var: String = try {
    cliArgs(4)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "field1"
    }
  }

  println(s"Master URL: $MASTER_URL")
  println(s"Source path: $SRC_PATH")
  println(s"Dest path: $DST_PATH")
  println(s"Valeur de group_var: $group_var")
  println(s"Valeur de group_var: $op_var")


  // Application

  val sparkSession = SparkSession
    .builder
    .master(MASTER_URL)
    .enableHiveSupport()
    .getOrCreate()

  val reader: Reader = SRC_PATH.split("\\.").lastOption match {
    case Some("csv") => new ReaderCSV(sparkSession)
    case Some("parquet") => new ReaderParquet(sparkSession)
    case _ =>
      println("Fichier non lisible.")
      sys.exit(1)
  }

  val processor: Processor = new ProcessorImpl(group_var, op_var)

  val writer: Writer = SRC_PATH.split("\\.").lastOption match {
    case Some("csv") => new WriterCSV()
    case Some("parquet") => new WriterParquet()
    case _ =>
      sys.exit(1)
  }

  val src_path = SRC_PATH
  val dst_path = DST_PATH

  val inputDF = reader.read(src_path)

  val groupbyDF = processor.groupby(inputDF)
  writer.write(groupbyDF, "overwrite", dst_path + "_groupby")

  val sumDF = processor.sum(inputDF)
  writer.write(sumDF, "overwrite", dst_path + "_sum")

  val meanDF = processor.mean(inputDF)
  writer.write(meanDF, "overwrite", dst_path + "_mean")

}

