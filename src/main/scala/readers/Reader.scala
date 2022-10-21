package readers

import org.apache.spark.sql.DataFrame

trait Reader {
  def read(relativePath: String): DataFrame
}
