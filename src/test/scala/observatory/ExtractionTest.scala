package observatory

import java.time.LocalDate

import observatory.Extraction.{Station, TempValue}
import org.apache.spark.sql.Dataset
import org.scalatest.FunSuite

trait ExtractionTest extends FunSuite {

  lazy val stations: Dataset[Station] = Extraction.readStations("/stations.csv").cache()
  lazy val temperatures: Dataset[TempValue] = Extraction.readTemperatures("/1975.csv", 1975).cache()
  lazy val located: Iterable[(LocalDate, Location, Temperature)] = Extraction.locateTemperatures(1975, "/stations.csv", "/1975.csv")
  lazy val average: Iterable[(Location, Temperature)] = Extraction.locationYearlyAverageRecords(located)

  test("stations extraction") {
    assert(stations.count() === 28128)
    assert(stations.filter((s: Station) => s.id_1 == "007005" & s.id_2 == "0").count() === 0)
    assert(stations.filter((s: Station) => s.id_1 == "007018" & s.id_2 == "0").count() === 1)
    assert(stations.filter((s: Station) => s.id_1 == "949999" & s.id_2 == "00122").count() === 1)
    val station = stations.filter((s: Station) => s.id_1 == "949999" & s.id_2 == "00122").take(1)(0)
    assert(station.latitude === -34.950)
    assert(station.longitude === 138.820)
  }

  test("temperatures extraction") {
    assert(temperatures.count() === 2190974)
    assert(temperatures.filter((s: TempValue) => s.id_1 == "574260" & s.id_2 == "0").count() === 363)
    val temperature = temperatures.filter((s: TempValue) => s.id_1 == "574260" & s.id_2 == "0" & s.year == 1975 & s.month == 9 & s.day == 30).take(1)(0);
    assert(temperature.temp === ((77.8 - 32) * 5 / 9))
  }

  test("locateTemperatures") {
    assert(located.size === 2177190)
    assert(located.count(l => l._2 == Location(21.517, +107.967)) === 323)
  }

  test("locationYearlyAverageRecords") {
    assert(average.size === 8253)
    assert(average.count(l => l._1 == Location(21.517, +107.967)) === 1)
    val av = average.find(l => l._1 == Location(21.517, +107.967)).get._2
    assert(av > 23.37D)
    assert(av < 23.38D)
  }
}