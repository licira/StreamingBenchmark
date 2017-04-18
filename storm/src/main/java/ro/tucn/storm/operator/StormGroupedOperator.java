package ro.tucn.storm.operator;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.GroupedWorkloadOperator;
import ro.tucn.operator.PairWorkloadOperator;
import ro.tucn.storm.bolt.PairReduceBolt;
import ro.tucn.storm.bolt.constants.BoltConstants;

/**
 * Created by Liviu on 4/17/2017.
 */
public class StormGroupedOperator<K, V> extends GroupedWorkloadOperator<K, V> {

    protected TopologyBuilder topologyBuilder;
    protected String preComponentId;

    public StormGroupedOperator(TopologyBuilder builder, String previousComponent, int parallelism) {
        super(parallelism);
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
    }

    @Override
    public PairWorkloadOperator<K, V> reduce(ReduceFunction<V> fun, String componentId, int parallelism) {
        topologyBuilder.setBolt(componentId,
                new PairReduceBolt<K, V>(fun),
                parallelism)
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("not implemented yet");
    }

    @Override
    public void print() {

    }
}
