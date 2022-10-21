import filters.RawVideoFilter
import org.apache.spark.sql.SparkSession
import processor.RawDataProcessor
import readers.CsvReader
import translators.{CategoryTranslator, StringIntegerTranslator, TagTranslator}

object Main {
  // Creating spark singleton.
  lazy val spark: SparkSession = {
    val session = SparkSession.builder().appName("Miner").config("spark.master", "local").getOrCreate()
    session.sparkContext.setLogLevel("OFF")
    session
  }

  def main(args: Array[String]): Unit = {
    val csvReader = new CsvReader(spark)
    val dataFrame = csvReader.read("datasets/youtube_dataset.csv")

    val rawDataProcessor = new RawDataProcessor(CategoryTranslator, TagTranslator, StringIntegerTranslator, RawVideoFilter)
    val processedVideoData = rawDataProcessor.process(dataFrame)

    println("====== Processed Data ======")
    processedVideoData.show(20)
  }
}