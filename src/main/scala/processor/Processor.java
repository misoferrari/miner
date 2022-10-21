package processor;

public interface Processor<TInput, TOutput> {
    TOutput process(TInput input);
}
