package translators

object StringIntegerTranslator extends Translator[String, Int] {
  def translate(input: String): Int =
    input.toInt
}
