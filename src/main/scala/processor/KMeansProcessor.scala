package processor

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame}

object KMeansProcessor extends Processor[DataFrame, Any] {
  private lazy val kmeans = new KMeans()

  def process(input: DataFrame): Any = {
    input.show()

    val model = kmeans.setFeaturesCol("features").fit(input)

    val predictions = model.transform(input)

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    predictions.show(10)
    predictions
      .groupBy("prediction")
      .avg("views", "title_negative_sentiment", "comment_count", "dislike_ratio")
      .show()

    // positive
    predictions
      .where(col("prediction") === 1)
      .select("prediction", "category", "views", "dislike_ratio")
      .groupBy("category", "prediction")
      .avg("views", "dislike_ratio")
      .orderBy(col("avg(views)"))
      .show()

    // negative
    predictions
      .where(col("prediction") === 0)
      .select("prediction", "category", "views", "dislike_ratio")
      .groupBy("category", "prediction")
      .avg("views", "dislike_ratio")
      .orderBy(col("avg(views)"))
      .show()
  }
}
