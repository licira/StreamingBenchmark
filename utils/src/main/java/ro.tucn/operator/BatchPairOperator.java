package ro.tucn.operator;

/**
 * Created by Liviu on 6/25/2017.
 */
public abstract class BatchPairOperator<K, V> extends PairOperator {

    public BatchPairOperator(int parallelism) {
        super(parallelism);
    }
}
