package ro.tucn.operator;

/**
 * Created by Liviu on 6/25/2017.
 */
public abstract class BatchOperator<T> extends BaseOperator {

    public BatchOperator(int parallelism) {
        super(parallelism);
    }
}
