package ro.tucn.spark.function;

import org.apache.spark.api.java.function.PairFunction;
import ro.tucn.frame.functions.MapPairFunction;
import ro.tucn.statistics.ThroughputLog;
import scala.Tuple2;

/**
 * Created by Liviu on 4/8/2017.
 */
public class PairFunctionImpl<T, K, V> implements PairFunction<T, K, V> {

    private MapPairFunction<T, K, V> function;
    private ThroughputLog throughput;
    private boolean enableThroughput;

    public PairFunctionImpl(MapPairFunction<T, K, V> function) {
        this.function = function;
    }

    public PairFunctionImpl(MapPairFunction<T, K, V> function, boolean enableThroughput) {
        this(function);
        this.enableThroughput = enableThroughput;
        throughput = new ThroughputLog(PairFunctionImpl.class.getSimpleName());
    }

    public Tuple2<K, V> call(T t) throws Exception {
        if (enableThroughput) {
            throughput.execute();
        }
        return function.mapToPair(t);
    }
}