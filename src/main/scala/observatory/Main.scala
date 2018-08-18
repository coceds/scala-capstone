package observatory

import java.io.File

import com.sksamuel.scrimage.Image

object Main extends App {


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

  val generateImage = (year: Year, tile: Tile, data: Iterable[(Location, Temperature)]) => {
    val image: Image = Interaction.tile(data, colors, tile)
    val file: File = new File(s"C:/projects/observatory-v2/observatory/images/temperatures/$year/${tile.zoom}/${tile.x}-${tile.y}.png")
    image.output(file)
    System.out.println("written " + file.getName)
  }

  Seq((1975, "/stations.csv", "/1975.csv"))
    .map(d => (d._1, Extraction.locateTemperatures(d._1, d._2, d._3)))
    .map(pair => List((pair._1, Extraction.locationYearlyAverageRecords(pair._2))))
    .foreach(yearlyData => Interaction.generateTiles[Iterable[(Location, Temperature)]](yearlyData, generateImage))

}
