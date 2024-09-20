import scala.collection.mutable.PriorityQueue
import scala.io.Source
import scala.math._
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure}

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

    val futures = points.map { point =>
      Future {
        val distance = point.distance(target)
        pq.synchronized {
          if (pq.size < k) {
            pq.enqueue((point, distance))
          } else if (distance < pq.head._2) {
            pq.dequeue()
            pq.enqueue((point, distance))
          }
        }
      }
    }

    // Wait for all futures to complete
    Await.result(Future.sequence(futures), Duration.Inf)

    pq.dequeueAll.toList.reverse.take(k)
  }

  def main(args: Array[String]): Unit = {
    val pointsDataset1 = loadPoints("C:\\Users\\User\\Desktop\\scala2\\coding\\coding\\PlaneSweepKNNProjectParallel\\src\\gaussian_10.csv")

    val target = Point(0.1, 1) // Παράδειγμα σημείου-στόχου
    val k = 7 // Παράδειγμα τιμής k

    // Χρονομέτρηση του αλγορίθμου για τα k κοντινότερα σημεία
    val startTime = System.nanoTime()
    val futureClosestPoints1 = Future { kClosestPoints(pointsDataset1, target, k) }

    val closestPoints1 = Await.result(futureClosestPoints1, Duration.Inf)
    val endTime = System.nanoTime()
    println(s"Closest $k points to $target in dataset 1: $closestPoints1")
    println(s"Execution time: ${(endTime - startTime) / 1e9} seconds")
  }
}
