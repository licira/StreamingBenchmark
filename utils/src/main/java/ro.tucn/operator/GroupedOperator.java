package ro.tucn.operator;

import ro.tucn.frame.functions.ReduceFunction;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class GroupedOperator<K, V> extends BaseOperator {

    public GroupedOperator(int parallelism) {
        super(parallelism);
    }

    public abstract StreamPairOperator<K, V> reduce(ReduceFunction<V> fun, String componentId, int parallelism);

    public abstract StreamOperator aggregateReduceByKey();
}