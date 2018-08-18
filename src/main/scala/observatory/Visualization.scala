package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import scala.collection.GenIterable
import scala.math._

/**
  * 2nd milestone: basic visualization
  */
object Visualization {

  private val AVERAGE_RADIUS_OF_EARTH_KM = 6371
  private val p = 2

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val distances = temperatures.map(v => (haversine(location.lat, location.lon, v._1.lat, v._1.lon), v._2))
    distances.find(_._1 < 1.0) match {
      case Some(a) => a._2
      case None => idv(distances)
    }
  }

  def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val dLat = (lat2 - lat1).toRadians
    val dLon = (lon2 - lon1).toRadians

    val a = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(lat1.toRadians) * cos(lat2.toRadians)
    val c = 2 * atan2(sqrt(a), sqrt(1 - a))
    AVERAGE_RADIUS_OF_EARTH_KM * c
  }

  def idv(distances: GenIterable[(Double, Double)]): Double = {
    val seqop = (acc: (Double, Double), value: (Double, Double)) => {
      val w = 1 / pow(value._1, p)
      (acc._1 + w * value._2, acc._2 + w)
    }
    val combop = (acc1: (Double, Double), acc2: (Double, Double)) => {
      (acc1._1 + acc2._1, acc1._2 + acc2._2)
    }
    val pair = distances.aggregate((0.0, 0.0))(seqop, combop)
    pair._1 / pair._2
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Seq[(Temperature, Color)], value: Temperature): Color = {
    //todo: binary search?
    val sorted = points //.toSeq.sortBy(_._1)

    var index = 0
    var less = -1
    while (index < sorted.length) {
      if (sorted(index)._1 == value) return sorted(index)._2
      if (sorted(index)._1 < value) less = index
      index = index + 1
    }
    if (less == -1) sorted.head._2
    else if (less == sorted.length - 1) sorted.last._2
    else interpolate(sorted(less), sorted(less + 1), value)
  }

  def interpolate(left: (Temperature, Color), right: (Temperature, Color), value: Temperature): Color = {
    val red = calculate(left._1, right._1, value, left._2.red, right._2.red)
    val green = calculate(left._1, right._1, value, left._2.green, right._2.green)
    val blue = calculate(left._1, right._1, value, left._2.blue, right._2.blue)
    Color(red, green, blue)
  }

  def calculate(left: Double, right: Double, value: Double, colorLeft: Int, colorRight: Int): Int = {
    round(colorLeft + (colorRight - colorLeft) * (value - left) / (right - left)).toInt
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Seq[(Temperature, Color)]): Image = {
    val width = 360
    val height = 180
    val xFactor = 360 / width
    val yFactor = 180 / height

    val pixels = (0 until height * width).par.map(point => {
      val x = point % width
      val y = point / width
      val temperature = predictTemperature(temperatures, Location(90 - y * yFactor, x * xFactor - 180))
      val color = interpolateColor(colors, temperature)
      Pixel(color.red, color.green, color.blue, 255)
    }).toArray

    Image(width, height, pixels)
  }


}

