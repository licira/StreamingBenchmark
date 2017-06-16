package ro.tucn.storm.operator;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.frame.functions.*;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.PairOperator;
import ro.tucn.operator.WindowedOperator;
import ro.tucn.operator.Operator;
import ro.tucn.storm.bolt.*;
import ro.tucn.util.TimeDuration;

import java.util.List;

/**
 * Created by Liviu on 4/17/2017.
 */
public class StormOperator<T> extends Operator<T> {

    protected TopologyBuilder topologyBuilder;
    protected String previousComponent;
    private BoltDeclarer boltDeclarer;

    public StormOperator(TopologyBuilder topologyBuilder, String previousComponent, int parallelism) {
        super(parallelism);
        this.topologyBuilder = topologyBuilder;
        this.previousComponent = previousComponent;
    }

    @Override
    public <R> Operator<R> map(MapFunction<T, R> fun, String componentId) {
        MapBolt<T, R> bolt = new MapBolt<>(fun);
        boltDeclarer = topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(previousComponent);
        return new StormOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <R> Operator<R> map(MapWithInitListFunction<T, R> fun, List<T> initList, String componentId) {
        MapWithInitListBolt<T, R> bolt = new MapWithInitListBolt<>(fun, initList);
        boltDeclarer = topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(previousComponent);
        return new StormOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <R> Operator<R> map(MapWithInitListFunction<T, R> fun, List<T> initList, String componentId, Class<R> outputClass) throws UnsupportOperatorException {
        return map(fun, initList, componentId);
    }

    @Override
    public <K, V> PairOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId) {
        MapToPairBolt<T, K, V> bolt = new MapToPairBolt<>(fun);
        boltDeclarer = topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(previousComponent);
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public Operator<T> reduce(ReduceFunction<T> fun, String componentId) {
        ReduceBolt<T> bolt = new ReduceBolt<>(fun);
        boltDeclarer = topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(previousComponent);
        return new StormOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public Operator<T> filter(FilterFunction<T> fun, String componentId) {
        FilterBolt<T> bolt = new FilterBolt<>(fun);
        boltDeclarer = topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(previousComponent);
        return new StormOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <R> Operator<R> flatMap(FlatMapFunction<T, R> fun, String componentId) {
        FlatMapBolt<T, R> bolt = new FlatMapBolt<>(fun);
        boltDeclarer = topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(previousComponent);
        return new StormOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <K, V> PairOperator<K, V> flatMapToPair(FlatMapPairFunction<T, K, V> fun, String componentId) {
        FlatMapToPairBolt<T, K, V> bolt = new FlatMapToPairBolt<>(fun);
        boltDeclarer = topologyBuilder.setBolt(componentId, bolt, parallelism).localOrShuffleGrouping(previousComponent);
        return new StormPairOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public <K, V> PairOperator<K, V> flatMapToPair(FlatMapPairFunction<T, K, V> fun) {
        return null;
    }

    @Override
    public WindowedOperator<T> window(TimeDuration windowDuration) {
        return window(windowDuration, windowDuration);
    }

    @Override
    public WindowedOperator<T> window(TimeDuration windowDuration, TimeDuration slideDuration) {
        return new StormWindowedOperator<T>(topologyBuilder, previousComponent, windowDuration, slideDuration, parallelism);
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        if (null == boltDeclarer) {
            throw new UnsupportOperatorException("boltDeclarer could not be null");
        } else if (!(stream.getClass().equals(this.getClass()))) {
            throw new UnsupportOperatorException("The close stream should be the same type of the origin stream");
        } else if (!(this.iterativeEnabled)) {
            throw new UnsupportOperatorException("Iterative is not enabled.");
        } else {
            StormOperator<T> streamClose = (StormOperator<T>) stream;
            if (broadcast) {
                boltDeclarer.allGrouping(streamClose.previousComponent);
            } else {
                boltDeclarer.shuffleGrouping(streamClose.previousComponent);
            }
        }
        iterativeClosed = true;
    }

    @Override
    public void print() {
        boltDeclarer = topologyBuilder.setBolt("print" + previousComponent, new PrintBolt<T>()).localOrShuffleGrouping(previousComponent);
    }

    @Override
    public void sink() {

    }

    @Override
    public PairOperator<String, Integer> flatMapToPair() {
        return null;
    }

    @Override
    public PairOperator mapToPair(Operator<T> centroids) {
        return null;
    }

    @Override
    public Operator map(Operator<T> points) {
        return null;
    }
}
