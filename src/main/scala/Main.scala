import filters.RawVideoFilter
import org.apache.spark.sql.SparkSession
import processor.{ControversialProcessor, KMeansProcessor, NaturalLanguageProcessor, RawDataProcessor}
import readers.CsvReader
import translators.{CategoryTranslator, StringIntegerTranslator, TagTranslator}

object Main {
  // Creating spark singleton.
  lazy val spark: SparkSession = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val session = SparkSession
      .builder()
      .appName("Miner")
      .config("spark.master", "local")
      .getOrCreate()

    session.sparkContext.setLogLevel("OFF")
    session
  }

  def main(args: Array[String]): Unit = {
    val csvReader = new CsvReader(spark)
    val dataFrame = csvReader.read("datasets/youtube_dataset.csv")

    val rawDataProcessor = new RawDataProcessor(CategoryTranslator, TagTranslator, StringIntegerTranslator, RawVideoFilter)
    val processedVideoData = rawDataProcessor.process(dataFrame)

    println("====== Processed Data ======")
    processedVideoData.show(1)

    val sentimentResult = NaturalLanguageProcessor.process(processedVideoData)
    sentimentResult.show(10)

    val controversialResult = ControversialProcessor.process(sentimentResult)
    controversialResult.show()

    KMeansProcessor.process(controversialResult)
  }
}