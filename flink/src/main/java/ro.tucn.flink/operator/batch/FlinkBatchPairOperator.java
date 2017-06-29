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


/**
 * Created by Liviu on 6/27/2017.
 */
public class FlinkBatchPairOperator<K, V> extends BatchPairOperator<K, V> {

    private static final Logger logger = Logger.getLogger(FlinkBatchPairOperator.class);

    private DataSet<Tuple2<K, V>> dataSet;

    public FlinkBatchPairOperator(DataSet<Tuple2<K, V>> dataSet, int parallelism) {
        super(parallelism);
        this.dataSet = dataSet;
    }

    @Override
    public <R> PairOperator<K, Tuple2<V, R>> advClick(PairOperator<K, R> joinOperator,
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
        });
        DataSet<Tuple2<String, String>> joinDataSet = joinPreDataSet.map(new MapFunction<Tuple2<K, R>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Tuple2<K, R> tuple) throws Exception {
                return new Tuple2<String, String>((String) tuple.f0, (String) tuple.f1);
            }
        });

        DataSet<Tuple2<String, String>> dataSet = preDataSet.map(new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Tuple2<String, String> tuple) throws Exception {
                return new Tuple2<String, String>(tuple.f0, tuple.f1);
            }
        });


        JoinOperator.DefaultJoin<org.apache.flink.api.java.tuple.Tuple2<String, String>,
                org.apache.flink.api.java.tuple.Tuple2<String, String>> join =
                dataSet.join(joinDataSet)
                .where("f0")
                .equalTo("f0");
        /*try {
            join.print();
        } catch (Exception e) {
            e.printStackTrace();
        }*/

        DataSet<Object> map = join.map(new MapFunction<org.apache.flink.api.java.tuple.Tuple2<org.apache.flink.api.java.tuple.Tuple2<String, String>, org.apache.flink.api.java.tuple.Tuple2<String, String>>, Object>() {
            @Override
            public org.apache.flink.api.java.tuple.Tuple2<String, org.apache.flink.api.java.tuple.Tuple2<String, String>> map(org.apache.flink.api.java.tuple.Tuple2<org.apache.flink.api.java.tuple.Tuple2<String, String>, org.apache.flink.api.java.tuple.Tuple2<String, String>> doubleTuple) throws Exception {
                org.apache.flink.api.java.tuple.Tuple2<String, String> t1 = doubleTuple.f0;
                org.apache.flink.api.java.tuple.Tuple2<String, String> t2 = doubleTuple.f1;
                String key = t1.f0;
                String value1 = t1.f1;
                String value2 = t2.f1;
                org.apache.flink.api.java.tuple.Tuple2<String, String> finalTuple = new org.apache.flink.api.java.tuple.Tuple2<String, String>(value1, value2);
                return new org.apache.flink.api.java.tuple.Tuple2<String, org.apache.flink.api.java.tuple.Tuple2<String, String>>(key, finalTuple);
            }
        });

        try {
            map.print();
        } catch (Exception e) {
            e.printStackTrace();
        }

        /*try {
            dataSet.print();
            joinDataSet.print();
        } catch (Exception e) {
            e.printStackTrace();
        }*/

        return null;
    }

    private <R> void checkOperatorType(PairOperator<K, R> joinOperator) throws WorkloadException {
        if (!(joinOperator instanceof FlinkBatchPairOperator)) {
            throw new WorkloadException("Cast joinStream to FlinkBatchPairOperator failed");
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
