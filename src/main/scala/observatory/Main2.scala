package observatory

import java.io.File
import java.nio.file.{Files, Paths}

import com.sksamuel.scrimage.Image

import scala.math.pow

object Main2 extends App {

  val colors: Seq[(Temperature, Color)] = Seq(
    (60d, Color(255, 255, 255)),
    (32d, Color(255, 0, 0)),
    (12d, Color(255, 255, 0)),
    (0d, Color(0, 255, 255)),
    (-15d, Color(0, 0, 255)),
    (-27d, Color(255, 0, 255)),
    (-50d, Color(33, 255, 107)),
    (-60d, Color(0, 0, 0))
  ).sortBy(_._1)

  val seq = for {
    year <- 1975 to 1975 //1989
  } yield (year, "/stations.csv", s"/${year}.csv")

  var stats: Iterable[Iterable[(Location, Temperature)]] = seq
    .map(d => Extraction.locateTemperatures(d._1, d._2, d._3))
    .map(p => Extraction.locationYearlyAverageRecords(p))

  val average = Manipulation.average(stats)
  stats = null

  val data = for {
    year <- 1975 to 2015 //2015
  } yield (year, "/stations.csv", s"/${year}.csv")

  data.foreach(d => {
    val located = Extraction.locateTemperatures(d._1, d._2, d._3)
    val yearly = Extraction.locationYearlyAverageRecords(located)
    val deviation = Manipulation.deviation(yearly, average)
    val tasks = for {
      zoom <- 0 to 3
      x <- 0 until pow(2.0, zoom).toInt
      y <- 0 until pow(2.0, zoom).toInt
    } yield (deviation, Tile(x, y, zoom))
    tasks.foreach(t => {
      val image: Image = Visualization2.visualizeGrid(t._1, colors, t._2)
      Files.createDirectories(Paths.get(s"C:/projects/observatory-v2/observatory/images/deviations/${d._1}/${t._2.zoom}"))
      val file: File = new File(s"C:/projects/observatory-v2/observatory/images/deviations/${d._1}/${t._2.zoom}/${t._2.x}-${t._2.y}.png")
      image.output(file)
      System.out.println("written " + file.getName)
    })
  })

  //  data
  //    .map(d => (d._1, Extraction.locateTemperatures(d._1, d._2, d._3)))
  //    .map(p => (p._1, Extraction.locationYearlyAverageRecords(p._2)))
  //    .map(d => (d._1, Manipulation.deviation(d._2, average)))
  //    .flatMap(grid => {
  //      val tasks = for {
  //        zoom <- 0 to 3
  //        x <- 0 until pow(2.0, zoom).toInt
  //        y <- 0 until pow(2.0, zoom).toInt
  //      } yield (grid, Tile(x, y, zoom))
  //      tasks
  //    }).foreach(p => {
  //    val image: Image = Visualization2.visualizeGrid(p._1._2, colors, p._2)
  //    Files.createDirectories(Paths.get(s"C:/projects/observatory-v2/observatory/images/deviations/${p._1._1}/${p._2.zoom}"))
  //    val file: File = new File(s"C:/projects/observatory-v2/observatory/images/deviations/${p._1._1}/${p._2.zoom}/${p._2.x}-${p._2.y}.png")
  //    image.output(file)
  //    System.out.println("written " + file.getName)
  //  })

}
