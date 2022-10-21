package filters

trait Filter[TInput, TOutput] extends Serializable {
  def filter(dataframe: TInput): TOutput
}
