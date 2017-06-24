package ro.tucn.spark.operator;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.dstream.DStream;
import scala.reflect.ClassTag;

/**
 * Created by Liviu on 6/23/2017.
 */
public class SerializableJavaDStream<T> extends JavaDStream<T> {

    public SerializableJavaDStream(DStream dstream, ClassTag classTag) {
        super(dstream, classTag);
    }
}
