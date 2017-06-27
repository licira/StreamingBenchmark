package ro.tucn.consumer;

/**
 * Created by Liviu on 6/27/2017.
 */
public abstract class AbstractConsumer {

    protected int parallelism;

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }
}
