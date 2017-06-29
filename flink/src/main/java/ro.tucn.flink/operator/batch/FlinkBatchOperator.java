package ro.tucn.flink.operator.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.BatchOperator;
import ro.tucn.operator.BatchPairOperator;

/**
 * Created by Liviu on 6/27/2017.
 */
public class FlinkBatchOperator<T> extends BatchOperator<T> {

    private static final Logger logger = Logger.getLogger(FlinkBatchOperator.class);

    DataSet<T> dataSet;

    public FlinkBatchOperator(int parallelism) {
        super(parallelism);
    }

    public FlinkBatchOperator(DataSet<T> dataSet, int parallelism) {
        super(parallelism);
        this.dataSet = dataSet;
    }

    @Override
    public BatchPairOperator<String, Integer> wordCount() {

        DataSet<Tuple2<String, Integer>> tTuple2FlatMapOperator = dataSet.flatMap(new FlatMapFunction<T, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(T t, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String sentence = (String) t;
                for (String word : sentence.split(" ")) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        });
        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = tTuple2FlatMapOperator.groupBy(0);
        DataSet<Tuple2<String, Integer>> reduce = tuple2UnsortedGrouping.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                return new Tuple2<String, Integer>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
            }
        });
        DataSet<scala.Tuple2<String, Integer>> map = reduce.map(new MapFunction<Tuple2<String, Integer>, scala.Tuple2<String, Integer>>() {
            @Override
            public scala.Tuple2<String, Integer> map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new scala.Tuple2<String, Integer>(stringIntegerTuple2.f0, stringIntegerTuple2.f1);
            }
        });
        return new FlinkBatchPairOperator<String, Integer>(map, parallelism);
    }

    @Override
    public void kMeansCluster(BatchOperator<T> centroids) throws WorkloadException {

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
