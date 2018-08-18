package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Visualization.interpolateColor

import scala.math.pow

/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 {

  /**
    * @param point (x, y) coordinates of a point in the grid cell
    * @param d00   Top-left value
    * @param d01   Bottom-left value
    * @param d10   Top-right value
    * @param d11   Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(point: CellPoint,
                            d00: Temperature,
                            d01: Temperature,
                            d10: Temperature,
                            d11: Temperature): Temperature = {
    (d00 * (1.0 - point.x) * (1.0 - point.y)) + (d10 * point.x * (1.0 - point.y)) +
      (d01 * (1.0 - point.x) * point.y) + (d11 * point.x * point.y)
  }

  /**
    * @param grid   Grid to visualize
    * @param colors Color scale to use
    * @param tile   Tile coordinates to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(grid: GridLocation => Temperature,
                    colors: Seq[(Temperature, Color)],
                    tile: Tile): Image = {
    val width = 256
    val height = 256
    val maxZoom = 8
    val xOffset = pow(2.0, maxZoom).toInt * tile.x
    val yOffset = pow(2.0, maxZoom).toInt * tile.y

    val pixels = (0 until height * width).par.map(point => {
      val currentX = xOffset + point % width
      val currentY = yOffset + point / width
      val l = Interaction.tileLocation(Tile(currentX, currentY, tile.zoom + maxZoom))
      val d00 = grid(GridLocation(l.lat.floor.toInt, l.lon.floor.toInt))
      val d01 = grid(GridLocation(l.lat.ceil.toInt, l.lon.floor.toInt))
      val d10 = grid(GridLocation(l.lat.floor.toInt, l.lon.ceil.toInt))
      val d11 = grid(GridLocation(l.lat.ceil.toInt, l.lon.ceil.toInt))

      val p = CellPoint(l.lon - l.lon.floor, l.lat - l.lat.floor)

      val temperature = bilinearInterpolation(p, d00, d01, d10, d11)
      val color = interpolateColor(colors, temperature)
      Pixel(color.red, color.green, color.blue, 127)
    }).toArray

    Image(width, height, pixels)
  }

}
