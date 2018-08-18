package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Visualization.{interpolateColor, predictTemperature}

/**
  * 3rd milestone: interactive visualization
  */
object Interaction {

  import scala.math._

  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = {
    Location(
      toDegrees(atan(sinh(Pi * (1.0 - 2.0 * tile.y.toDouble / (1 << tile.zoom))))),
      tile.x.toDouble / (1 << tile.zoom) * 360.0 - 180.0
    )
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @param tile         Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Seq[(Temperature, Color)], tile: Tile): Image = {
    println(s"data set size: ${temperatures.size}")
    println(s"generating a tile: ${tile.x} - ${tile.y} - ${tile.zoom}")
    val width = 128
    val height = 128
    val maxZoom = 7

    //zoom+maxZoom coordinate system: 1 tile - 1 pixel
    val xOffset = pow(2.0, maxZoom).toInt * tile.x
    val yOffset = pow(2.0, maxZoom).toInt * tile.y

    val pixels = (0 until height * width).par.map(point => {
      val currentX = xOffset + point % width
      val currentY = yOffset + point / width
      val temperature = predictTemperature(temperatures, tileLocation(Tile(currentX, currentY, tile.zoom + maxZoom)))
      val color = interpolateColor(colors, temperature)
      Pixel(color.red, color.green, color.blue, 127)
    }).toArray

    Image(width, height, pixels)
  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    *
    * @param yearlyData    Sequence of (year, data), where `data` is some data associated with
    *                      `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
                           yearlyData: Iterable[(Year, Data)],
                           generateImage: (Year, Tile, Data) => Unit
                         ): Unit = {

    val tasks = for {
      (year, data) <- yearlyData
      zoom <- 0 to 3
      x <- 0 until pow(2.0, zoom).toInt
      y <- 0 until pow(2.0, zoom).toInt
    } yield (year, Tile(x, y, zoom), data)

    tasks.foreach(task => generateImage(task._1, task._2, task._3))
  }

}
