package translators

trait Translator[TInput, TOutput] extends Serializable {
  def translate(input: TInput): TOutput
}
