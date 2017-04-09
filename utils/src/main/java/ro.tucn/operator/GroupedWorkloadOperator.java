package ro.tucn.operator;

import ro.tucn.frame.functions.ReduceFunction;

/**
 * Created by Liviu on 4/8/2017.
 */
abstract public class GroupedWorkloadOperator<K, V> extends BaseOperator {

    public GroupedWorkloadOperator(int parallelism) {
        super(parallelism);
    }

    abstract public PairWorkloadOperator<K, V> reduce(ReduceFunction<V> fun, String componentId, int parallelism);
}