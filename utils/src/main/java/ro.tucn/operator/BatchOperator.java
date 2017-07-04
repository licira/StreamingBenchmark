package ro.tucn.operator;

/**
 * Created by Liviu on 6/25/2017.
 */
public abstract class BatchOperator<T> extends Operator {

    public BatchOperator(int parallelism) {
        super(parallelism);
    }

    public abstract BatchPairOperator<String, Integer> wordCount();
}
