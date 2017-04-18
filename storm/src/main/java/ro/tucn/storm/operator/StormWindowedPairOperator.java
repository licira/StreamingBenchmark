package ro.tucn.storm.operator;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import ro.tucn.exceptions.DurationException;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.frame.functions.FilterFunction;
import ro.tucn.frame.functions.MapFunction;
import ro.tucn.frame.functions.MapPartitionFunction;
import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.PairWorkloadOperator;
import ro.tucn.operator.WindowedPairWorkloadOperator;
import ro.tucn.storm.bolt.PairPrintBolt;
import ro.tucn.storm.bolt.constants.BoltConstants;
import ro.tucn.storm.bolt.windowed.*;
import ro.tucn.util.TimeDuration;
import scala.Tuple2;

/**
 * Created by Liviu on 4/17/2017.
 */
public class StormWindowedPairOperator<K, V> extends WindowedPairWorkloadOperator<K, V> {

    private TopologyBuilder topologyBuilder;
    private String preComponentId;
    private TimeDuration windowDuration;
    private TimeDuration slideDuration;

    /**
     * @param builder
     * @param previousComponent
     * @param windowDuration
     * @param slideDuration
     */
    public StormWindowedPairOperator(TopologyBuilder builder, String previousComponent, TimeDuration windowDuration,
                                     TimeDuration slideDuration, int parallelism) {
        super(parallelism);
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
        this.windowDuration = windowDuration;
        this.slideDuration = slideDuration;
    }


    @Override
    public PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId) {
        try {
            WindowPairReduceByKeyBolt<K, V> bolt = new WindowPairReduceByKeyBolt<>(fun, windowDuration, slideDuration);
            topologyBuilder.setBolt(componentId, bolt, parallelism)
                    .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    /**
     * It seems that no one will call this function
     *
     * @param fun
     * @param componentId
     * @return
     */
    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId) {
//        try {
//            topologyBuilder.setBolt(componentId, new UpdateStateBolt<>(fun, windowDuration, slideDuration))
//                    .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
//        } catch (DurationException e) {
//            e.printStackTrace();
//        }
//        return new StormPairOperator<>(topologyBuilder, componentId);
        return null;
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapPartition(MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun,
                                                       String componentId) {
        try {
            WindowPairMapPartitionBolt<K, V, R> bolt = new WindowPairMapPartitionBolt<>(fun, windowDuration, slideDuration);
            topologyBuilder.setBolt(componentId, bolt, parallelism)
                    .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId) {
        try {
            WindowMapValueBolt<K, V, R> bolt = new WindowMapValueBolt<>(fun, windowDuration, slideDuration);
            topologyBuilder.setBolt(componentId, bolt, parallelism)
                    .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId) {
        try {
            WindowPairFilterBolt<K, V> bolt = new WindowPairFilterBolt<>(fun, windowDuration, slideDuration);
            topologyBuilder.setBolt(componentId, bolt, parallelism)
                    .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public PairWorkloadOperator<K, V> reduce(ReduceFunction<Tuple2<K, V>> fun, String componentId) {
        try {
            WindowPairReduceBolt<K, V> bolt = new WindowPairReduceBolt<>(fun, windowDuration, slideDuration);
            topologyBuilder.setBolt(componentId, bolt, parallelism)
                    .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("not implemented yet");
    }

    @Override
    public void print() {
        topologyBuilder.setBolt("print", new PairPrintBolt<>(true)).localOrShuffleGrouping(preComponentId);
    }
}
