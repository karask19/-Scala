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

  def kClosestPoints(points: List[Point], target: Point, k: Int): List[Point] = {
    implicit val ord: Ordering[Point] = Ordering.by(p => -p.distance(target))
    val pq: PriorityQueue[Point] = PriorityQueue()(ord)

    points.foreach { point =>
      if (pq.size < k) {
        pq.enqueue(point)
      } else {
        if (point.distance(target) < pq.head.distance(target)) {
          pq.dequeue()
          pq.enqueue(point)
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
     println(s"i=$i")
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
    val pointsDataset1 = loadPoints("C:\\Users\\User\\Desktop\\scala2\\coding\\coding\\PlaneSweepKNNProjectNonParallel\\src\\bit_10.csv")
    //val pointsDataset2 = loadPoints("C:\\Users\\User\\Desktop\\scala2\\coding\\coding\\laneSweepKNNProjectParallel\\src\\bit_50.csv\")


    val target = Point(1.0, 1.0) // Παράδειγμα σημείου-στόχου
    val k = 5 // Παράδειγμα τιμής k


    val closestPoints1 = kClosestPoints(pointsDataset1, target, k)
    println(s"Closest $k points to $target in dataset 1: $closestPoints1")

   // val closestPoints2 = kClosestPoints(pointsDataset2, target, k)
    //println(s"Closest $k points to $target in dataset 2: $closestPoints2")

    val (p1, p2, distance) = planeSweep(pointsDataset1)
    println(s"Closest points in dataset 1: ($p1, $p2) with distance ${distance} meters")

    //val (p3, p4, distance2) = planeSweep(pointsDataset2)
   //println(s"Closest points in dataset 2: ($p3, $p4) with distance ${distance2} meters")
  }
}
