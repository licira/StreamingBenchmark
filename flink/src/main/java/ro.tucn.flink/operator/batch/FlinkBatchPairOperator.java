package ro.tucn.flink.operator.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.log4j.Logger;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.BatchPairOperator;
import ro.tucn.operator.PairOperator;
import ro.tucn.util.TimeDuration;

import static ro.tucn.exceptions.ExceptionMessage.FAILED_TO_CAST_OPERATOR_MSG;


/**
 * Created by Liviu on 6/27/2017.
 */
public class FlinkBatchPairOperator<K, V> extends BatchPairOperator<K, V> {

    private static final Logger logger = Logger.getLogger(FlinkBatchPairOperator.class);

    private DataSet<Tuple2<K, V>> dataSet;

    public FlinkBatchPairOperator(DataSet<Tuple2<K, V>> dataSet, int parallelism) {
        super(parallelism);
        this.dataSet = dataSet;
        frameworkName = "FLINK";
    }

    @Override
    public <R> FlinkBatchPairOperator<K, Tuple2<V, R>> advClick(PairOperator<K, R> joinOperator,
                                                      TimeDuration windowDuration,
                                                      TimeDuration joinWindowDuration) throws WorkloadException {
        checkWindowDurationsCompatibility(windowDuration, joinWindowDuration);
        checkOperatorType(joinOperator);

        DataSet<Tuple2<K, R>> joinPreDataSet = ((FlinkBatchPairOperator<K, R>) joinOperator).dataSet;
        DataSet<Tuple2<String, String>> preDataSet = this.dataSet.map(new MapFunction<Tuple2<K, V>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Tuple2<K, V> tuple) throws Exception {
                return new Tuple2<String, String>((String) tuple.f0, (String) tuple.f1);
            }
        }).setParallelism(parallelism);

        DataSet<Tuple2<String, String>> joinDataSet = joinPreDataSet.map(new MapFunction<Tuple2<K, R>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Tuple2<K, R> tuple) throws Exception {
                return new Tuple2<String, String>((String) tuple.f0, (String) tuple.f1);
            }
        }).setParallelism(parallelism);
        DataSet<Tuple2<String, String>> dataSet = preDataSet.map(new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Tuple2<String, String> tuple) throws Exception {
                return new Tuple2<String, String>(tuple.f0, tuple.f1);
            }
        }).setParallelism(parallelism);
        /*
        performanceLog.disablePrint();
        performanceLog.setStartTime(TimeHelper.getNanoTime());
        */
        JoinOperator.DefaultJoin<Tuple2<String, String>, Tuple2<String, String>> join = dataSet.join(joinDataSet)
                .where("f0")
                .equalTo("f0");
        /*
        performanceLog.logLatency(TimeHelper.getNanoTime());
        performanceLog.logTotalLatency();
        executionLatency = performanceLog.getTotalLatency();
        */
        DataSet advClick = join.map(new MapFunction<Tuple2<Tuple2<String, String>, Tuple2<String, String>>, Object>() {
            @Override
            public Tuple2<String, Tuple2<String, String>> map(Tuple2<Tuple2<String, String>, Tuple2<String, String>> doubleTuple) throws Exception {
                Tuple2<String, String> t1 = doubleTuple.f0;
                Tuple2<String, String> t2 = doubleTuple.f1;
                String key = t1.f0;
                String value1 = t1.f1;
                String value2 = t2.f1;
                Tuple2<String, String> finalTuple = new Tuple2<String, String>(value1, value2);
                return new Tuple2<String, Tuple2<String, String>>(key, finalTuple);
            }
        });

        FlinkBatchPairOperator advClickOperator = new FlinkBatchPairOperator<>(advClick, parallelism);
        advClickOperator.setExecutionLatency(executionLatency);
        return advClickOperator;
    }

    private <R> void checkOperatorType(PairOperator<K, R> joinOperator) throws WorkloadException {
        if (!(joinOperator instanceof FlinkBatchPairOperator)) {
            throw new WorkloadException(FAILED_TO_CAST_OPERATOR_MSG + getClass().getSimpleName());
        }
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {

    }

    @Override
    public void print() {
        try {
            dataSet.print();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
