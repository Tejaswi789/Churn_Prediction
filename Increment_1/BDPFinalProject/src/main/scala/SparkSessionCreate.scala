import org.apache.spark.sql.SparkSession

object SparkSessionCreate {
  def createSession(appName:String):

  SparkSession = {
    System.setProperty("hadoop.home.dir","C:\\winutils");
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName(appName)
      .getOrCreate()

    return spark
  }
}