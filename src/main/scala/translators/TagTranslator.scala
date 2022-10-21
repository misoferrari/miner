package translators

object TagTranslator extends Translator[String, Array[String]] {
  def translate(tags: String): Array[String] =
    tags.split('|').map(tag => tag.replace("\"", ""))
}
