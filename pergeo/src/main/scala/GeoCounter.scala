import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.commons.net.util.SubnetUtils
import java.util.Properties

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object GeoCounter {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CategoryCounter")
    val sparkContext = new SparkContext(conf)
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    val connectionProperties = new Properties()
    connectionProperties.put("driver", "com.mysql.jdbc.Driver")
    connectionProperties.put("url", "jdbc:mysql://10.0.0.21:3306/snikitin")
    connectionProperties.put("user", "snikitin")

    val ipv4 = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load("hdfs:///user/snikitin/geo/ips.csv")

    val locations = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load("hdfs:///user/snikitin/geo/locations.csv")

    val countries = ipv4.join(locations, usingColumn = "geoname_id").select("network", "country_name")

    val textFiles = sparkContext.textFile("hdfs://" + args(0))

    val splited = textFiles.map(_.split(","))

    val filtered = splited.map(x => Row(x(4), x(1).toInt))

    val schema = StructType($"ip".string :: $"price".int :: Nil)

    val dataFrame = sparkSession.createDataFrame(filtered, schema)

    val isInRange = org.apache.spark.sql.functions.udf((ip : String, network : String) => {
      new SubnetUtils(network).getInfo.isInRange(ip)
    })

    val joined = dataFrame.join(countries, isInRange(dataFrame("ip"), countries("network")))

    val salesWithCountry = joined.select("country_name", "price")

    val counted = salesWithCountry.groupBy($"country_name").agg(sum($"price").alias("price"))

    val sorted = counted.sort($"price".desc).limit(10)

    sorted.write.jdbc(connectionProperties.getProperty("url"), "spark_sales_per_country", connectionProperties)
  }
}