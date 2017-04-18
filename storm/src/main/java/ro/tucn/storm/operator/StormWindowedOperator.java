package ro.tucn.storm.operator;

import backtype.storm.topology.TopologyBuilder;
import ro.tucn.exceptions.DurationException;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.frame.functions.*;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.PairWorkloadOperator;
import ro.tucn.operator.WindowedWorkloadOperator;
import ro.tucn.operator.WorkloadOperator;
import ro.tucn.storm.bolt.PrintBolt;
import ro.tucn.storm.bolt.windowed.*;
import ro.tucn.util.TimeDuration;

/**
 * Created by Liviu on 4/17/2017.
 */
public class StormWindowedOperator<T> extends WindowedWorkloadOperator<T> {

    private TimeDuration windowDuration;
    private TimeDuration slideDuration;

    protected TopologyBuilder topologyBuilder;
    protected String preComponentId;

    public StormWindowedOperator(TopologyBuilder builder, String previousComponent, int parallelism) {
        super(parallelism);
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
    }

    public StormWindowedOperator(TopologyBuilder builder, String previousComponent,
                                 TimeDuration windowDuration, TimeDuration slideDuration,
                                 int parallelism) {
        super(parallelism);
        this.topologyBuilder = builder;
        this.preComponentId = previousComponent;
        this.windowDuration = windowDuration;
        this.slideDuration = slideDuration;
    }

    @Override
    public <R> WorkloadOperator<R> mapPartition(MapPartitionFunction<T, R> fun, String componentId) {
        try {
            WindowMapPartitionBolt<T, R> bolt = new WindowMapPartitionBolt<>(fun, windowDuration, slideDuration);
            topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(preComponentId);
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <R> WorkloadOperator<R> map(MapFunction<T, R> fun, String componentId) {
        try {
            WindowMapBolt<T, R> bolt = new WindowMapBolt<>(fun, windowDuration, slideDuration);
            topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(preComponentId);
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public WorkloadOperator<T> filter(FilterFunction<T> fun, String componentId) {
        try {
            WindowFilterBolt<T> bolt = new WindowFilterBolt<>(fun, windowDuration, slideDuration);
            topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(preComponentId);
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public WorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId) {
        try {
            WindowReduceBolt<T> bolt = new WindowReduceBolt<>(fun, windowDuration, slideDuration);
            topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(preComponentId);
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId) {
        try {
            WindowMapToPairBolt<T, K, V> bolt = new WindowMapToPairBolt<>(fun, windowDuration, slideDuration);
            topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(preComponentId);
        } catch (DurationException e) {
            e.printStackTrace();
        }
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("Not implemented yet");
    }

    @Override
    public void print() {
        topologyBuilder.setBolt("print", new PrintBolt<>(true)).localOrShuffleGrouping(preComponentId);
    }
}