import org.apache.spark.sql.SparkSession

object Main extends App {

  val sparkSession = SparkSession
    .builder
    .master("local[1]")
    .enableHiveSupport()
    .getOrCreate()

  print(sparkSession.sql("SELECT 'A'").show())
}