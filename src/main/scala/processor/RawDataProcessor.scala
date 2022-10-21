package processor

import cases.{ProcessedVideoData, RawVideoData}
import filters.Filter
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}
import translators.Translator

class RawDataProcessor(
                        private val categoryTranslator: Translator[String, String],
                        private val tagTranslator: Translator[String, Array[String]],
                        private val stringIntegerTranslator: Translator[String, Int],
                        private val rawVideoFilter: Filter[DataFrame, DataFrame])
  extends Processor[DataFrame, Dataset[ProcessedVideoData]] with Serializable {

  def process(input: DataFrame): Dataset[ProcessedVideoData] = {
    val filteredDataFrame = rawVideoFilter.filter(input)
    println("====== Filtered Data ======")
    filteredDataFrame.show(1)

    val rawVideoData = filteredDataFrame.as[RawVideoData](Encoders.product[RawVideoData])
    println("====== RawData Data ======")
    rawVideoData.show(1)

    rawVideoData.map(convertRawIntoProcessed)(Encoders.product[ProcessedVideoData])
  }

  private def convertRawIntoProcessed(rawVideoData: RawVideoData) = {
    val title = rawVideoData.title
    val category = categoryTranslator.translate(rawVideoData.category_id)
    val tags = tagTranslator.translate(rawVideoData.tags)
    val views = stringIntegerTranslator.translate(rawVideoData.views)
    val likes = stringIntegerTranslator.translate(rawVideoData.likes)
    val dislikes = stringIntegerTranslator.translate(rawVideoData.dislikes)
    val comment_count = stringIntegerTranslator.translate(rawVideoData.comment_count)

    ProcessedVideoData(title, category, tags, views, likes, dislikes, comment_count)
  }
}
