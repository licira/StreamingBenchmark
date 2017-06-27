package ro.tucn.flink.operator;

import org.apache.flink.api.java.DataSet;
import org.apache.log4j.Logger;
import ro.tucn.operator.BatchPairOperator;
import scala.Tuple2;

/**
 * Created by Liviu on 6/27/2017.
 */
public class FlinkBatchPairOperator<K, V> extends BatchPairOperator<K, V> {

    private static final Logger logger = Logger.getLogger(FlinkBatchPairOperator.class);

    private DataSet<Tuple2<K, V>> dataSet;

    public FlinkBatchPairOperator(int parallelism) {
        super(parallelism);
    }
}
