package ro.tucn.spark.function.functions;

import org.apache.log4j.Logger;
import ro.tucn.logger.SerializableLogger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;
import org.apache.log4j.Logger;
import ro.tucn.logger.SerializableLogger;

import java.util.List;

/**
 * Created by Liviu on 4/8/2017.
 */
public class PrintFunctionImpl<T> implements Function2<JavaRDD<T>, Time, Void> {

    private static final Logger logger = Logger.getLogger(PrintFunctionImpl.class.getSimpleName());

    public Void call(JavaRDD<T> tJavaRDD, Time time) throws Exception {

        List<T> list = tJavaRDD.take(10);
        // scalastyle:off println
        logger.warn("-------------------------------------------");
        logger.warn("Time: " + time);
        logger.warn("-------------------------------------------");
        for (T t : list) {
            logger.warn(t.toString());
        }
        logger.warn("\n");
        return null;
    }
}
