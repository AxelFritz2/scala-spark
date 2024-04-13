package fr.mosef.scala.template

import fr.mosef.scala.template.job.Job
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.{ReaderCSV, ReaderHive, ReaderParquet}
import fr.mosef.scala.template.writer.Writer
import org.apache.spark.sql.SparkSession
import fr.mosef.scala.template.writer.impl.{WriterCSV, WriterParquet, WriterHive}

object Main extends App with Job {

  val cliArgs = args

  val MASTER_URL: String = try {
    cliArgs(0)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => "local[1]"
  }
  println(MASTER_URL)

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

  val Hive: Boolean = try {
    cliArgs(5).toBoolean
  } catch {
    case e: Exception =>
      false
  }

  val app_prop: String = try {
    cliArgs(6)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./src/main/resources/application.properties"
    }
  }

  println("************************************************************************************************")
  println("Args")
  println("************************************************************************************************")
  println(s"Master URL: $MASTER_URL")
  println(s"Source path: $SRC_PATH")
  println(s"Dest path: $DST_PATH")
  println(s"Valeur de group_var: $group_var")
  println(s"Valeur de group_var: $op_var")
  println(s"App path: $app_prop")


  // Application

  val sparkSession = SparkSession
    .builder
    .master(MASTER_URL)
    .enableHiveSupport()
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

  val fileformat = SRC_PATH.split("\\.").lastOption

  val reader: Reader = if (Hive) {
    fileformat match {
      case Some(format) => new ReaderHive(sparkSession, app_prop, format)
    }
  } else {
    fileformat match {
      case Some("csv") => new ReaderCSV(sparkSession, app_prop)
      case Some("parquet") => new ReaderParquet(sparkSession, app_prop)
      case _ =>
        println("Format de fichier non pris en charge.")
        sys.exit(1)
    }
  }
  val processor: Processor = new ProcessorImpl(group_var, op_var)

  val inputDF = reader.read(SRC_PATH)

  println("************************************************************************************************")
  println("Données d'Entrée")
  println("************************************************************************************************")
  inputDF.show(5)

  val groupbyDF = processor.groupby(inputDF)
  val sumDF = processor.sum(inputDF)
  val meanDF = processor.mean(inputDF)

  if (!Hive) {
    val dst_path = DST_PATH

    val writer: Writer = fileformat match {
      case Some("csv") => new WriterCSV(app_prop)
      case Some("parquet") => new WriterParquet()
      case _ => sys.exit(1)
    }

    println("************************************************************************************************")
    println("GroupBy")
    println("************************************************************************************************")
    groupbyDF.show(5)
    println("************************************************************************************************")
    println("Sum")
    println("************************************************************************************************")
    sumDF.show(5)
    println("************************************************************************************************")
    println("Mean")
    println("************************************************************************************************")
    meanDF.show(5)

    writer.write(groupbyDF, "overwrite", dst_path + "_groupby")
    writer.write(sumDF, "overwrite", dst_path + "_sum")
    writer.write(meanDF, "overwrite", dst_path + "_mean")
  } else {
    val writer = new WriterHive(sparkSession)

    writer.write(groupbyDF, "table_groupby")
    writer.write(sumDF, "table_sum")
    writer.write(meanDF, "table_mean")
  }

}

