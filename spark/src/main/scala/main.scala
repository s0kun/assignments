package api

import loader.loader
import loader.FMT // loader 'package' or loader 'object' ???


import org.apache.spark.sql.SparkSession

object api {
  // 'sbt run' requires "MAIN" method of form: "main: (Array[String]) => Unit"
  def main(args: Array[String]) = {

    val spark = SparkSession.builder()
      .appName("covidAnalysis")
      .master("local[*]")
      .getOrCreate()

    // Load
    val table = loader(spark)

    // table.show(5)
    // +-----+----------+---------+-----+
    // |state|      date|      cat|count|
    // +-----+----------+---------+-----+
    // |   AN|2020-03-26|confirmed|  1.0|
    // |   AN|2020-03-27|confirmed|  6.0|
    // |   AN|2020-03-28|confirmed|  9.0|
    // |   AN|2020-03-29|confirmed|  9.0|
    // |   AN|2020-03-30|confirmed| 10.0|
    // +-----+----------+---------+-----+

    // Analyse:




  }
}