import org.apache.spark.sql.{Encoder, Encoders}
import scala.collection.mutable

case class RawYoutubeVideo(
                            title: String,
                            category_id: String,
                            tags: String,
                            views: String,
                            likes: String,
                            dislikes: String,
                            comment_count: String)

case class ProcessedYoutubeVideo(
                             private val title: String,
                             private val category_id: String,
                             private val tags: String,
                             private val views: String,
                             private val likes: String,
                             private val dislikes: String,
                             private val comment_count: String) {

  private def toCategory(category_id: String): String = {
    val categories = mutable.HashMap(
      "2" -> "Autos & Vehicles",
      "1" -> "Film & Animation",
      "10" -> "Music",
      "15" -> "Pets & Animals",
      "17" -> "Sports",
      "18" -> "Short Movies",
      "19" -> "Travel & Events",
      "20" -> "Gaming",
      "21" -> "Vlog",
      "22" -> "People & Blogs",
      "23" -> "Comedy",
      "24" -> "Entertainment",
      "25" -> "News & Politics",
      "26" -> "Howto & Style",
      "27" -> "Education",
      "28" -> "Science & Technology",
      "29" -> "Nonprofits & Activism",
      "30" -> "Movies",
      "31" -> "Anime / Animation",
      "32" -> "Action / Adventure",
      "33" -> "Classics",
      "34" -> "Comedy",
      "35" -> "Documentary",
      "36" -> "Drama",
      "37" -> "Family",
      "38" -> "Foreign",
      "39" -> "Horror",
      "40" -> "Sci_Fi / Fantasy",
      "41" -> "Thriller",
      "42" -> "Shorts",
      "43" -> "Shows",
      "44" -> "Trailers")
    categories.getOrElse(category_id, "Unknown")
  }

  private def toTags(tags: String): Array[String] = {
    tags
      .split('|')
      .map(tag => tag.replace("\"", ""))
  }

  val Title = title
  lazy val Category = toCategory(category_id)
  lazy val Tags = toTags(tags)
  lazy val Views = views.toInt
  lazy val Likes = likes.toInt
  lazy val Dislikes = dislikes.toInt
  lazy val CommentCount = comment_count.toInt
}

object RawYoutubeVideo {
  implicit val RawYoutubeEncoder: Encoder[RawYoutubeVideo] = Encoders.product[RawYoutubeVideo]
  implicit val ProcessedYoutubeEncoder: Encoder[ProcessedYoutubeVideo] = Encoders.product[ProcessedYoutubeVideo]
}