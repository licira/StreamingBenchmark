package ro.tucn.operator;

import static ro.tucn.DataMode.STREAMING;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class StreamOperator<T> extends Operator {

    public StreamOperator(int parallelism) {
        super(parallelism);
        dataMode = STREAMING;
    }

    public abstract StreamPairOperator<String, Integer> wordCount();
}