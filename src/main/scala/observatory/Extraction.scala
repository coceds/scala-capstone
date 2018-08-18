package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{Dataset, SparkSession}


/**
  * 1st milestone: data extraction
  */
object Extraction {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Capstone")
      .master("local[4]")
      .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    System.out.println(s"locateTemperatures $year")
    val stations = readStations(stationsFile)
    val temperatures = readTemperatures(temperaturesFile, year)
    stations.join(temperatures, Seq("id_1", "id_2")).as[TempValueWithStation]
      .collect()
      .map(value => (LocalDate.of(value.year, value.month, value.day), Location(value.latitude, value.longitude), value.temp))
  }

  def readStations(stationsFile: String): Dataset[Station] = {
    spark.read.csv(Paths.get(getClass.getResource(stationsFile).toURI).toString)
      .select(
        coalesce($"_c0", lit("0")).name("id_1"),
        coalesce($"_c1", lit("0")).name("id_2"),
        $"_c2".name("latitude").cast(DoubleType),
        $"_c3".name("longitude").cast(DoubleType)
      ).where($"_c2".isNotNull && $"_c3".isNotNull)
      .as[Station]
  }

  def readTemperatures(temperaturesFile: String, year: Year): Dataset[TempValue] = {
    spark.read.csv(Paths.get(getClass.getResource(temperaturesFile).toURI).toString)
      .select(
        coalesce($"_c0", lit("0")).name("id_1"),
        coalesce($"_c1", lit("0")).name("id_2"),
        lit(year).name("year").cast(IntegerType),
        $"_c2".name("month").cast(IntegerType),
        $"_c3".name("day").cast(IntegerType),
        (($"_c4" - 32) * 5 / 9).name("temp").cast(DoubleType)
      ).as[TempValue]
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    System.out.println("locationYearlyAverageRecords")
    //    //in order to avoid: No Encoder found for java.time.LocalDate... supported in spark 2.2+
    //    // just get rid of it
    //    val rdd: RDD[(Double, Double, Double)] = spark.sparkContext.parallelize(
    //      records.map(r => (r._2.lat, r._2.lon, r._3)).toList
    //    )
    //    val df = spark.createDataFrame(rdd)
    //    val ds = df.as[(Double, Double, Double)]
    //
    //    ds.groupByKey(row => (row._1, row._2))
    //      .agg(
    //        typed.avg[(Double, Double, Double)](r => r._3).as[Double].name("average")
    //      ).as[((Double, Double), Temperature)]
    //      .map(v => (Location(v._1._1, v._1._2), v._2))
    //      .collect()

    records.groupBy(value => value._2)
      .mapValues(values => values.foldLeft(0d)((acc: Double, v: (LocalDate, Location, Temperature)) => acc + v._3) / values.size)
  }


  case class Station(id_1: String, id_2: String, latitude: Double, longitude: Double)

  case class TempValue(id_1: String, id_2: String, year: Int, month: Int, day: Int, temp: Double)

  case class TempValueWithStation(id_1: String, id_2: String, latitude: Double, longitude: Double, year: Int, month: Int, day: Int, temp: Double)

}
