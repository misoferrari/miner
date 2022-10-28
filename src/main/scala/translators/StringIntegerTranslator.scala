package translators

object StringIntegerTranslator extends Translator[String, Int] {
  def translate(input: String): Int = {
    try {
      Some(input.toInt).x
    } catch {
      case e: Exception => 0
    }
  }
}
