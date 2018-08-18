package observatory

import scala.collection.parallel.ParMap

/**
  * 4th milestone: value-added information
  */
object Manipulation {

  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature = {
    System.out.println("start makeGrid")
    //    val gg: ParMap[GridLocation, Supplier[Double]] = (for {
    //      x <- -89 to 90
    //      y <- -180 to 179
    //    } yield (x, y))
    //      .par
    //      .map(pair => GridLocation(pair._1, pair._2) -> Suppliers.memoize(
    //        new Supplier[Double] {
    //          override def get(): Temperature = Visualization.predictTemperature(temperatures, Location(pair._1.toDouble, pair._1.toDouble))
    //        }
    //      ))
    //      .toMap

    val map: ParMap[(Int, Int), Double] = (for {
      x <- -29 to 30
      y <- -60 to 59
    } yield (x, y))
      .par
      .map(pair => pair -> Visualization.predictTemperature(temperatures, Location(pair._1.toDouble * 3, pair._2.toDouble * 3)))
      .toMap

    //    val gg: ParMap[GridLocation, Double] = (for {
    //      x <- -89 to 90
    //      y <- -180 to 179
    //    } yield (x, y))
    //      .par
    //      .map(pair => GridLocation(pair._1, pair._2) -> Visualization.predictTemperature(temperatures, Location(pair._1.toDouble, pair._1.toDouble)))
    //      .toMap
    //gg(_)

    System.out.println("end makeGrid")
    (gridLocation: GridLocation) => {
      val lat = if (gridLocation.lat == -90) 90 else gridLocation.lat
      val lon = if (gridLocation.lon == 180) -180 else gridLocation.lon
      map((lat / 3, lon / 3))
    }
  }

  /**
    * @param temperaturess Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperaturess: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {
    val grids = temperaturess.map(perYear => makeGrid(perYear))
    (gridLocation) => {
      val temperaturesInLocation = grids.map(g => g(gridLocation))
      temperaturesInLocation.sum / temperaturesInLocation.size
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param normals      A grid containing the “normal” temperatures
    * @return A grid containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Temperature)], normals: GridLocation => Temperature): GridLocation => Temperature = {
    val grid = makeGrid(temperatures)
    (gridLocation) => grid(gridLocation) - normals(gridLocation)
  }
}

