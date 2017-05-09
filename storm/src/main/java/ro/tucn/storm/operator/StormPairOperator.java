package ro.tucn.storm.operator;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import ro.tucn.exceptions.DurationException;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.frame.functions.*;
import ro.tucn.operator.*;
import ro.tucn.storm.bolt.*;
import ro.tucn.storm.bolt.constants.BoltConstants;
import ro.tucn.storm.bolt.discretized.DiscretizedPairReduceByKeyBolt;
import ro.tucn.storm.bolt.windowed.WindowPairReduceByKeyBolt;
import ro.tucn.util.TimeDuration;
import scala.Tuple2;

/**
 * Created by Liviu on 4/17/2017.
 */
public class StormPairOperator<K, V> extends PairOperator<K, V> {

    protected TopologyBuilder topologyBuilder;
    protected String preComponentId;

    public StormPairOperator(TopologyBuilder builder, String previousComponent, int parallelism) {
        super(parallelism);
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
    }

    @Override
    public GroupedOperator<K, V> groupByKey() {
        return new StormGroupedOperator<>(topologyBuilder, this.preComponentId, parallelism);
    }

    // Set bolt with fieldsGrouping
    @Override
    public PairOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId) {
        PairReduceBolt<K, V> bolt = new PairReduceBolt<>(fun);
        topologyBuilder.setBolt(componentId, bolt, parallelism)
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <R> PairOperator<K, R> mapValue(MapFunction<V, R> fun, String componentId) {
        MapValueBolt<V, R> bolt = new MapValueBolt<>(fun);
        topologyBuilder.setBolt(componentId, bolt, parallelism)
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <R> Operator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId) {
        PairMapBolt<K, V, R> bolt = new PairMapBolt<>(fun);
        topologyBuilder.setBolt(componentId, bolt, parallelism)
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        return new StormOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <R> Operator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId, Class<R> outputClass) {
        return map(fun, componentId);
    }

    @Override
    public <R> PairOperator<K, R> flatMapValue(FlatMapFunction<V, R> fun, String componentId) {
        FlatMapValueBolt<V, R> bolt = new FlatMapValueBolt<>(fun);
        topologyBuilder.setBolt(componentId, bolt, parallelism)
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public PairOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId) {
        PairFilterBolt<K, V> bolt = new PairFilterBolt<>(fun);
        topologyBuilder.setBolt(componentId, bolt, parallelism)
                .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField));
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    // Set bolt with fieldsGrouping
    @Override
    public PairOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId) {
        return this;
    }

    @Override
    public PairOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDuration windowDuration) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration, windowDuration);
    }

    @Override
    public PairOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDuration windowDuration, TimeDuration slideDuration) {
        try {
            WindowPairReduceByKeyBolt<K, V> reduceByKeyBolt
                    = new WindowPairReduceByKeyBolt<>(fun, windowDuration, slideDuration);
            reduceByKeyBolt.enableThroughput("ReduceByKey");
            topologyBuilder.setBolt(componentId + "-local",
                    reduceByKeyBolt,
                    parallelism)
                    .localOrShuffleGrouping(preComponentId);
            topologyBuilder.setBolt(componentId,
                    new DiscretizedPairReduceByKeyBolt<>(fun, componentId + "-local"),
                    parallelism)
                    .fieldsGrouping(componentId + "-local", new Fields(BoltConstants.OutputKeyField))
                    .allGrouping(componentId + "-local", BoltConstants.TICK_STREAM_ID);
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public WindowedPairOperator<K, V> window(TimeDuration windowDuration) {
        return window(windowDuration, windowDuration);
    }

    @Override
    public WindowedPairOperator<K, V> window(TimeDuration windowDuration, TimeDuration slideDuration) {
        return new StormWindowedPairOperator<>(topologyBuilder, preComponentId, windowDuration, slideDuration, parallelism);
    }

    @Override
    public <R> PairOperator<K, Tuple2<V, R>> join(String componentId,
                                                          PairOperator<K, R> joinStream,
                                                          TimeDuration windowDuration,
                                                          TimeDuration joinWindowDuration) throws WorkloadException {

        if (joinStream instanceof StormPairOperator) {
            StormPairOperator<K, R> joinStormStream = (StormPairOperator<K, R>) joinStream;
            topologyBuilder.setBolt(componentId,
                    new JoinBolt<>(this.preComponentId, windowDuration, joinStormStream.preComponentId, joinWindowDuration),
                    parallelism)
                    .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField))
                    .fieldsGrouping(joinStormStream.preComponentId, new Fields(BoltConstants.OutputKeyField));
            return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
        }
        throw new WorkloadException("Cast joinStrem to StormPairOperator failed");
    }

    // Event time join
    @Override
    public <R> PairOperator<K, Tuple2<V, R>> join(String componentId, PairOperator<K, R> joinStream,
                                                          TimeDuration windowDuration, TimeDuration joinWindowDuration,
                                                          AssignTimeFunction<V> eventTimeAssigner1, AssignTimeFunction<R> eventTimeAssigner2) throws WorkloadException {
        if (joinStream instanceof StormPairOperator) {
            StormPairOperator<K, R> joinStormStream = (StormPairOperator<K, R>) joinStream;
            topologyBuilder
                    .setBolt(componentId,
                            new JoinBolt<>(this.preComponentId, windowDuration,
                                    joinStormStream.preComponentId, joinWindowDuration,
                                    eventTimeAssigner1, eventTimeAssigner2),
                            parallelism)
                    .fieldsGrouping(preComponentId, new Fields(BoltConstants.OutputKeyField))
                    .fieldsGrouping(joinStormStream.preComponentId, new Fields(BoltConstants.OutputKeyField));
            return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
        }
        throw new WorkloadException("Cast joinStream to StormPairOperator failed");
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("not implemented yet");
    }

    @Override
    public void print() {
        topologyBuilder.setBolt("print", new PairPrintBolt<>()).localOrShuffleGrouping(preComponentId);
    }

    @Override
    public void sink() {
        topologyBuilder.setBolt("latency", new PairLatencyBolt<>(), parallelism).localOrShuffleGrouping(preComponentId);

    }

    @Override
    public void count() {

    }
}
