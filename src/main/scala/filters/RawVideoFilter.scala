package filters

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object RawVideoFilter extends Filter[DataFrame, DataFrame] {
  def filter(dataframe: DataFrame) =
    dataframe
      .distinct()
      .where(col("title").isNotNull
        && col("category_id").isNotNull
        && col("tags").isNotNull
        && col("views").isNotNull
        && col("likes").isNotNull
        && col("dislikes").isNotNull
        && col("comment_count").isNotNull)
}
