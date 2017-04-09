package ro.tucn.operator;

import ro.tucn.exceptions.UnsupportOperatorException;

/**
 * Created by Liviu on 4/8/2017.
 */
abstract public class BaseOperator {

    protected int parallelism = -1;
    protected boolean iterativeEnabled = false;

    public BaseOperator(int parallelism) {
        this.setParallelism(parallelism);
    }

    public int getParallelism() {
        return this.parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public void iterative() {
        this.iterativeEnabled = true;
    }

    abstract public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException;

    abstract public void print();
}