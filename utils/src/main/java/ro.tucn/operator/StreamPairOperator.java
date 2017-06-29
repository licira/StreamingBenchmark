package ro.tucn.operator;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class StreamPairOperator<K, V> extends PairOperator<K, V> {

    public StreamPairOperator(int parallelism) {
        super(parallelism);
    }
}

