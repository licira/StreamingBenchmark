package ro.tucn.operator;

import ro.tucn.exceptions.UnsupportOperatorException;

/**
 * Created by Liviu on 6/25/2017.
 */
public class BatchPairOperator<K, V> extends BaseOperator {

    public BatchPairOperator(int parallelism) {
        super(parallelism);
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {

    }

    @Override
    public void print() {

    }
}
