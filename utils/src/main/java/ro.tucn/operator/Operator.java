package ro.tucn.operator;

/**
 * Created by Liviu on 6/27/2017.
 */
public abstract class Operator<T> extends BaseOperator {

    public Operator(int parallelism) {
        super(parallelism);
    }
}
