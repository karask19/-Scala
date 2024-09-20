import scala.collection.mutable.PriorityQueue
import scala.io.Source
import scala.math._

case class Point(x: Double, y: Double) {
  def distance(other: Point): Double = {
    val latDist = (this.x - other.x)
    val lonDist = (this.y - other.y)
    sqrt(pow(latDist, 2) + pow(lonDist, 2))
  }
}

object PlaneSweep {
  def loadPoints(filePath: String): List[Point] = {
    val lines = Source.fromFile(filePath).getLines()
    lines.map { line =>
      val coords = line.split(",")
      Point(coords(0).toDouble, coords(1).toDouble)
    }.toList
  }

  def kClosestPoints(points: List[Point], target: Point, k: Int): List[(Point, Double)] = {
    implicit val ord: Ordering[(Point, Double)] = Ordering.by { case (_, distance) => -distance }
    val pq: PriorityQueue[(Point, Double)] = PriorityQueue()(ord)

    points.foreach { point =>
      val distance = point.distance(target)
      if (pq.size < k) {
        pq.enqueue((point, distance))
      } else {
        if (distance < pq.head._2) {
          pq.dequeue()
          pq.enqueue((point, distance))
        }
      }
    }

    pq.dequeueAll.toList.reverse
  }

  def planeSweep(points: List[Point]): (Point, Point, Double) = {
    val sortedPoints = points.sortBy(_.x)
    var minDistance = Double.MaxValue
    var closestPair: (Point, Point) = (sortedPoints(0), sortedPoints(1))

    for (i <- sortedPoints.indices) {
      val currentPoint = sortedPoints(i)
      for (j <- (i + 1) until sortedPoints.length) {
        val nextPoint = sortedPoints(j)
        if ((nextPoint.x - currentPoint.x) > minDistance) {
          j == sortedPoints.length
        } else {
          val distance = currentPoint.distance(nextPoint)
          if (distance < minDistance) {
            minDistance = distance
            closestPair = (currentPoint, nextPoint)
          }
        }
      }
    }

    (closestPair._1, closestPair._2, minDistance)
  }

  def main(args: Array[String]): Unit = {
    val pointsDataset1 = loadPoints("C:\\Users\\User\\Desktop\\scala2\\coding\\coding\\PlaneSweepKNNProjectNonParallel\\src\\bit_100.csv")
    val startTime = System.nanoTime()

    val target = Point(0.1, 1) // Παράδειγμα σημείου-στόχου
    val k = 5 // Παράδειγμα τιμής k

    val closestPoints1 = kClosestPoints(pointsDataset1, target, k)
    val endTime = System.nanoTime()

    // Print the closest points with their distances
    println(s"Closest $k points to $target in dataset 1:")
    closestPoints1.foreach { case (point, distance) =>
      println(s"$point with distance $distance")
    }

    println(s"Execution time: ${(endTime - startTime) / 1e9} seconds")
  }
}
