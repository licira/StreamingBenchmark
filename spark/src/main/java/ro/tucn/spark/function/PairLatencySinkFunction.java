package ro.tucn.spark.function;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import ro.tucn.statistics.LatencyLog;
import ro.tucn.util.WithTime;
import scala.Tuple2;

/**
 * Created by Liviu on 4/5/2017.
 */
public class PairLatencySinkFunction<K, V> implements Function<Tuple2<K, V>, Boolean> {

    private static Logger logger = Logger.getLogger(PairLatencySinkFunction.class.getSimpleName());
    private LatencyLog latency;

    public PairLatencySinkFunction() {
        latency = new LatencyLog(PairLatencySinkFunction.class.getSimpleName());
    }

    public Boolean call(Tuple2<K, V> tuple2) throws Exception {
        latency.execute((WithTime) tuple2._2());
        return true;
    }
}
