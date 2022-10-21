package readers

import org.apache.spark.sql.SparkSession

class CsvReader(private val spark: SparkSession) extends Reader {
  def read(relativePath: String) =
    spark.read.option("header", value = true).csv(relativePath)
}
