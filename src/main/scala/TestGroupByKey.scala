import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec

object TestGroupByKey {


  case class Sales(region: String, country: String, itemType: String, salesChannel: String, orderPriority: String, orderDate: java.sql.Date, unitsSold: Integer)
  case class RolledUpSales(region: String, orderPriority: String, unitsSold: Integer)

  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder()
      .config("SPARK_HOME", "/home/ometaxas/Projects/spark-3.0.0-bin-hadoop3.2")
      .config("spark.master", "local[*]")
      //.config("spark.local.dir", "/media/ometaxas/nvme/spark")
      .config("spark.local.dir", "/media/datadisk/Datasets/Spark")
      .config("spark.driver.memory", "110g")
      .appName("TestGroupByKey")
      .getOrCreate()

    session.sparkContext.setLogLevel("WARN")

    import session.implicits._
    import org.apache.spark.sql.functions._

    // 2
    val sales = session
      .read.option("delimiter", ",").option("header", "true").option("inferSchema", "true")
      .csv(s"/media/ometaxas/nvme/datasets/MAGsample/Sales_Records.csv")
      .select(
        $"Region".as("region"),
        $"Country".as("country"),
        $"Item Type".as("itemType"),
        $"Sales Channel".as("salesChannel"),
        $"Order Priority".as("orderPriority"),
        to_date($"Order Date", "M/d/yyyy").as("orderDate"),
        $"Units Sold".as("unitsSold")).as[Sales]


    // 3
    val output = sales.groupByKey(item => item.region).flatMapGroups(rollUpSales)
    output.show(false)
  }

  private def rollUpSales(region: String, sales: Iterator[Sales]): Seq[RolledUpSales] = {

    // 4
    val sortedDataset = sales.toSeq.sortWith((a, b) => a.orderDate.before(b.orderDate))

    // 5
    @tailrec
    def rollUp(items: List[Sales], accumulator: Seq[RolledUpSales]): Seq[RolledUpSales] = {
      items match {
        case x::xs =>
          val matchingPriority = xs.takeWhile(p => p.orderPriority.equalsIgnoreCase(x.orderPriority))
          val nonMatchingPriority = xs.dropWhile(p => p.orderPriority.equalsIgnoreCase(x.orderPriority))
          val record = RolledUpSales(region, x.orderPriority, matchingPriority.map(_.unitsSold).foldLeft(x.unitsSold)(_ + _))
          val rolledUpRecord = record +: accumulator
          rollUp(nonMatchingPriority, rolledUpRecord)
        case Nil => accumulator
      }
    }

    rollUp(sortedDataset.toList, Seq.empty).reverse
  }
}
