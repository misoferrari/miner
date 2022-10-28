package processor

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object ControversialProcessor extends Processor[DataFrame, DataFrame] {
  def process(input: DataFrame): DataFrame = {
    val withDislikeRatio = input
      .withColumn("dislike_ratio", col("dislikes").cast("double").divide(col("likes")).cast("double"))
      .where(col("dislike_ratio").isNotNull)

    new VectorAssembler()
      .setInputCols(Array("title_negative_sentiment", "dislike_ratio"))
      .setOutputCol("features")
      .transform(withDislikeRatio)
  }
}
