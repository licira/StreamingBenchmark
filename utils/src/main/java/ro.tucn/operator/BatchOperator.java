package ro.tucn.operator;

import static ro.tucn.DataMode.BATCH;

/**
 * Created by Liviu on 6/25/2017.
 */
public abstract class BatchOperator<T> extends Operator {

    public BatchOperator(int parallelism) {
        super(parallelism);
        dataMode = BATCH;
    }

    public abstract BatchPairOperator<String, Integer> wordCount();
}
