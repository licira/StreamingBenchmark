package ro.tucn.frame.functions;

/**
 * Created by Liviu on 4/8/2017.
 */
// flink
//public interface FlatMapFunction {
//    public void flatMap(String value, Iterable<Tuple2<String, Integer>> out);
//}

// spark
public interface FlatMapFunction<T, R> extends SerializableFunction {

    Iterable<R> flatMap(T var1) throws Exception;
}