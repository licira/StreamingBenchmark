package ro.tucn.spark.operator.stream;

import org.apache.spark.streaming.api.java.JavaDStream;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.StreamWindowedOperator;

/**
 * Created by Liviu on 4/5/2017.
 */
public class SparkStreamWindowedOperator<T> extends StreamWindowedOperator<T> {

    private JavaDStream<T> dStream;

    public SparkStreamWindowedOperator(JavaDStream<T> stream, int parallelism) {
        super(parallelism);
        dStream = stream;
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("Operator not supported");
    }

    @Override
    public void print() {
        dStream.print();
    }
}