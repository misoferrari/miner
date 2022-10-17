import RawYoutubeVideo.{ProcessedYoutubeEncoder, RawYoutubeEncoder}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Main {
  // Creating spark singleton.
  lazy val spark: SparkSession = {
    val session = SparkSession
      .builder()
      .appName("Miner")
      .config("spark.master", "local")
      .getOrCreate()

    session.sparkContext.setLogLevel("OFF")
    session
  }

  private def selectData = {
    val datasetPath = "datasets/youtube_dataset.csv"

    // Reading youtube dataframe.
    spark
      .read
      .option("header", value = true)
      .csv(datasetPath)
  }

  private def filterData(dataframe: DataFrame) = {
    dataframe
      .as[RawYoutubeVideo]
      .distinct()
      .filter(video => video.title != null
        && video.category_id != null
        && video.tags != null
        && video.views != null
        && video.likes != null
        && video.dislikes != null
        && video.comment_count != null)
      .as[ProcessedYoutubeVideo]
  }

  private def showCategoriesPer(dataset: Dataset[ProcessedYoutubeVideo], field: String): Unit = {
    dataset
      .orderBy(field)
      .select("Category", field)
      .show(10)
  }

  def main(args: Array[String]): Unit = {
    val dataframe = selectData
    val processedDataframe = filterData(dataframe)

    showCategoriesPer(processedDataframe, "Likes")
  }
}