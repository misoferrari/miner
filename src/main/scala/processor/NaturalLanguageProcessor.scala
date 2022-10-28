package processor

import cases.ProcessedVideoData
import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
import com.johnsnowlabs.nlp.annotators.sda.vivekn.ViveknSentimentModel
import com.johnsnowlabs.nlp.annotators.{Normalizer, Tokenizer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset}

object NaturalLanguageProcessor extends Processor[Dataset[ProcessedVideoData], DataFrame] {
  private lazy val document = new DocumentAssembler()
    .setInputCol("title")
    .setOutputCol("document")

  private lazy val token = new Tokenizer()
    .setInputCols("document")
    .setOutputCol("token")

  private lazy val normalizer = new Normalizer()
    .setInputCols("token")
    .setOutputCol("normal")

  private lazy val vivekn = ViveknSentimentModel
    .pretrained()
    .setInputCols("document", "normal")
    .setOutputCol("raw_sentiment")

  private lazy val finisher = new Finisher()
    .setInputCols("raw_sentiment")
    .setOutputCols("sentiment")

  private lazy val pipeline = new Pipeline()
    .setStages(Array(document, token, normalizer, vivekn, finisher))

  def process(processedVideoData: Dataset[ProcessedVideoData]): DataFrame =
    pipeline
      .fit(processedVideoData)
      .transform(processedVideoData)
      .where(col("sentiment") === Array("positive") || col("sentiment") === Array("negative"))
      .withColumn("title_negative_sentiment", (col("sentiment") === Array("negative")).cast("Int"))
      .where(col("title_negative_sentiment").isNotNull)
}
