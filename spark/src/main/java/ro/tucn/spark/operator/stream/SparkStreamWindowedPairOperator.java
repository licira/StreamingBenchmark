package ro.tucn.spark.operator.stream;

import org.apache.spark.streaming.api.java.JavaPairDStream;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.StreamWindowedPairOperator;

/**
 * Created by Liviu on 4/8/2017.
 */
public class SparkStreamWindowedPairOperator<K, V> extends StreamWindowedPairOperator<K, V> {

    private JavaPairDStream<K, V> pairDStream;

    public SparkStreamWindowedPairOperator(JavaPairDStream<K, V> stream, int parallelism) {
        super(parallelism);
        this.pairDStream = stream;
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("Operator not supported");
    }

    @Override
    public void print() {
        pairDStream.print();
    }
}