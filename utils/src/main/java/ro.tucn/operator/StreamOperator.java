package ro.tucn.operator;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class StreamOperator<T> extends Operator {

    public StreamOperator(int parallelism) {
        super(parallelism);
    }

    public abstract StreamPairOperator<String, Integer> wordCount();
}