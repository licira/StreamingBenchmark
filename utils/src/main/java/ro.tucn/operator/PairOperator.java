package ro.tucn.operator;

/**
 * Created by Liviu on 6/27/2017.
 */
public abstract class PairOperator<K, V> extends BaseOperator {

    public PairOperator(int parallelism) {
        super(parallelism);
    }
}
