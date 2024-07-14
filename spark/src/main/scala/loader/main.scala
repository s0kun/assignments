package loader

import sttp.client4.quick._
import sttp.client4.Response

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object loader {
  def apply(spark: SparkSession): DataFrame = {

    val addr = uri"https://data.covid19india.org/v4/min/timeseries.min.json"
    val response: Response[String] = quickRequest
      .get(addr)
      .send()

    val r = ujson.read(response.body)
    val T = flatten(r)

    // for {row <- T.zip(1 to 10)} yield {println(row._1)}
    // println(s"Total ${T.size} elements")

    import spark.implicits._

    // val data = spark.sparkContext.parallelize(T).filter(filter_records).map(fmt_data).toDF
    // data.printSchema()
    // root
    // |-- state: string (nullable = true)
    // |-- date: string (nullable = true)
    // |-- cat: string (nullable = true)
    // |-- count: double (nullable = false)

    val data = spark.sparkContext
      .parallelize(T)
      .filter(filter_records)
      .map(fmt_data)
      .toDF
      .withColumn("date",col("date").cast(DateType))

    return data
  }

  // Sample row: List(AN, dates, 2020-03-26, delta, confirmed, 1.0)
  case class FMT(state: String, date: String, cat: String, count: Double)
  val NULL_FMT = FMT("","","",0)

  // O(N*D) complexity ; N : Number of leaves ; D : Depth of Tree ;
  def flatten(X: ujson.Value): List[List[String]] = {
    X match {
      case ujson.Obj(keyMap) => keyMap.toList.flatMap( X => flatten(X._2).map(S => X._1 :: S))
      case ujson.Arr(sqnc) => sqnc.flatMap( X => flatten(X)).toList

      // Leaf-Vertices : Types
      case ujson.Null => Nil
      case ujson.Num(x) => List(List(x.toString))
      case ujson.Str(x) => List(List(x.toString))
      case ujson.Bool(x) => List(List(x.toString))
    }
  }

  def filter_records(row: List[String]): Boolean = {
    row match {
      case st :: ignr :: dt :: typ :: unit :: count :: other => (typ == "total")
      case x => {println(s"Incompatible format: $x") ; false}
    }
  }

  def fmt_data(row: List[String]): FMT = {
    row match {
      case st :: ignr :: dt :: typ :: unit :: count :: other => FMT(st, dt, unit, count.toDouble)
      case x => {{println(s"Incompatible format: $x") ; NULL_FMT}}
    }
  }
}
object noise { val a = 3 ; println("Does this matter?") } // No
