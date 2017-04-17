package ro.tucn.flink.operator;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.flink.function.MapFunctionWithInitList;
import ro.tucn.frame.functions.*;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.PairWorkloadOperator;
import ro.tucn.operator.WindowedWorkloadOperator;
import ro.tucn.operator.WorkloadOperator;
import ro.tucn.statistics.PerformanceLog;
import ro.tucn.util.TimeDuration;
import ro.tucn.util.WithTime;
import scala.Tuple2;

import java.util.List;

/**
 * Created by Liviu on 4/17/2017.
 */
public class FlinkWorkloadOperator<T> extends WorkloadOperator<T> {

    protected DataStream<T> dataStream;
    private IterativeStream<T> iterativeStream;

    public FlinkWorkloadOperator(DataStream<T> dataSet, int parallelism) {
        super(parallelism);
        dataStream = dataSet;
    }

    @Override
    public <R> WorkloadOperator<R> map(final MapFunction<T, R> fun, String componentId) {
        DataStream<R> newDataStream = dataStream.map(new org.apache.flink.api.common.functions.MapFunction<T, R>() {
            public R map(T t) throws Exception {
                return fun.map(t);
            }
        });
        return new FlinkWorkloadOperator<>(newDataStream, getParallelism());
    }

    @Override
    public <R> WorkloadOperator<R> map(final MapWithInitListFunction<T, R> fun, List<T> initList, String componentId) throws UnsupportOperatorException {
        final MapFunctionWithInitList<T, R> map = new MapFunctionWithInitList<>(fun, initList);
        TypeExtractor.getForClass(Point.class);
        TypeInformation<R> outType = TypeExtractor.getMapReturnTypes(dataStream.getExecutionEnvironment().clean(map), dataStream.getType(),
                Utils.getCallLocationName(), true);

        DataStream<R> newDataStream;
        if (this.iterativeEnabled) {
            iterativeStream = dataStream.iterate();
            newDataStream =
                    iterativeStream.transform("Map", outType, new FlinkPointAssignMapOperator<>(dataStream.getExecutionEnvironment().clean(map)));

        } else {
            newDataStream =
                    dataStream.transform("Map", outType, new FlinkPointAssignMapOperator<>(dataStream.getExecutionEnvironment().clean(map)));
        }
        return new FlinkWorkloadOperator<>(newDataStream, getParallelism());
    }

    @Override
    public <R> WorkloadOperator<R> map(MapWithInitListFunction<T, R> fun, List<T> initList, String componentId, Class<R> outputClass) throws UnsupportOperatorException {
        final MapFunctionWithInitList<T, R> map = new MapFunctionWithInitList<>(fun, initList);
        TypeInformation<R> outType = TypeExtractor.getForClass(outputClass);
        DataStream<R> newDataStream;
        if (this.iterativeEnabled) {
            iterativeStream = dataStream.iterate();
            newDataStream =
                    iterativeStream.transform("Map",
                            outType,
                            new FlinkPointAssignMapOperator<>(dataStream.getExecutionEnvironment().clean(map)));
        } else {
            newDataStream =
                    dataStream.transform("Map",
                            outType,
                            new FlinkPointAssignMapOperator<>(dataStream.getExecutionEnvironment().clean(map)));
        }
        return new FlinkWorkloadOperator<>(newDataStream, getParallelism());
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> mapToPair(final MapPairFunction<T, K, V> fun, String componentId) {
        DataStream<Tuple2<K, V>> newDataStream = dataStream.map(new org.apache.flink.api.common.functions.MapFunction<T, Tuple2<K, V>>() {
            public Tuple2<K, V> map(T t) throws Exception {
                scala.Tuple2<K, V> tuple2 = fun.mapToPair(t);
                return new Tuple2<>(tuple2._1(), tuple2._2());
            }
        });
        return new FlinkPairWorkloadOperator<K, V>(newDataStream, getParallelism());
    }

    @Override
    public WorkloadOperator<T> reduce(final ReduceFunction<T> fun, String componentId) {
        DataStream<T> newDataStream = dataStream.keyBy(0).reduce(new org.apache.flink.api.common.functions.ReduceFunction<T>() {
            public T reduce(T t, T t1) throws Exception {
                return fun.reduce(t, t1);
            }
        });
        return new FlinkWorkloadOperator<>(newDataStream, getParallelism());
    }

    @Override
    public WorkloadOperator<T> filter(final FilterFunction<T> fun, String componentId) {
        DataStream<T> newDataStream = dataStream.filter(new org.apache.flink.api.common.functions.FilterFunction<T>() {
            public boolean filter(T t) throws Exception {
                return fun.filter(t);
            }
        });
        return new FlinkWorkloadOperator<>(newDataStream, getParallelism());
    }

    @Override
    public <R> WorkloadOperator<R> flatMap(final FlatMapFunction<T, R> fun, String componentId) {
        TypeInformation<R> returnType = TypeExtractor.createTypeInfo(FlatMapFunction.class, fun.getClass(), 1, null, null);
        DataStream<R> newDataStream = dataStream.flatMap(new org.apache.flink.api.common.functions.FlatMapFunction<T, R>() {
            public void flatMap(T t, Collector<R> collector) throws Exception {
                Iterable<R> flatResults = (Iterable<R>) fun.flatMap(t);
                for (R r : flatResults) {
                    collector.collect(r);
                }
            }
        }).returns(returnType);
        return new FlinkWorkloadOperator<>(newDataStream, getParallelism());
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> flatMapToPair(final FlatMapPairFunction<T, K, V> fun,
                                                           String componentId) {
        //TypeInformation returnType = TypeExtractor.createTypeInfo(FlatMapFunction.class, fun.getClass(), 1, null, null);
        DataStream<Tuple2<K, V>> newDataStream = dataStream.flatMap(new org.apache.flink.api.common.functions.FlatMapFunction<T, Tuple2<K, V>>() {
            public void flatMap(T t, Collector<Tuple2<K, V>> collector) throws Exception {
                java.lang.Iterable<Tuple2<K, V>> flatResults = fun.flatMapToPair(t);
                for (Tuple2<K, V> tuple2 : flatResults) {
                    collector.collect(tuple2);
                }
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public WindowedWorkloadOperator<T> window(TimeDuration windowDuration) {
        return window(windowDuration, windowDuration);
    }

    @Override
    public WindowedWorkloadOperator<T> window(TimeDuration windowDuration, TimeDuration slideDuration) {
        WindowedStream<T, T, TimeWindow> windowedStream = dataStream.keyBy(new KeySelector<T, T>() {
            @Override
            public T getKey(T value) throws Exception {
                return value;
            }
        }).timeWindow(Time.of(windowDuration.getLength(), windowDuration.getUnit()),
                Time.of(slideDuration.getLength(), slideDuration.getUnit()));
        return new FlinkWindowedWorkloadOperator<>(windowedStream, parallelism);
    }

    @Override
    public void closeWith(BaseOperator operator, boolean broadcast) throws UnsupportOperatorException {
        if (null == iterativeStream) {
            throw new UnsupportOperatorException("iterativeStream could not be null");
        } else if (!operator.getClass().equals(this.getClass())) {
            throw new UnsupportOperatorException("The close stream should be the same type of the origin stream");
        } else if (!this.iterativeEnabled) {
            throw new UnsupportOperatorException("Iterative is not enabled.");
        } else {
            FlinkWorkloadOperator<T> operator_close = (FlinkWorkloadOperator<T>) operator;
            if (broadcast) {
                iterativeStream.closeWith(operator_close.dataStream.broadcast());
            } else {
                iterativeStream.closeWith(operator_close.dataStream);
            }
        }
        this.iterativeClosed = true;
    }

    public void print() {
        this.dataStream.print();
    }

    @Override
    public void sink() {
        this.dataStream.addSink(new org.apache.flink.streaming.api.functions.sink.SinkFunction<T>() {
            private PerformanceLog performanceLog = PerformanceLog.getLogger("sink");

            @Override
            public void invoke(T value) throws Exception {
                performanceLog.logThroughputAndLatencyWithTime((WithTime<? extends Object>) value);
            }
        });
    }
}
