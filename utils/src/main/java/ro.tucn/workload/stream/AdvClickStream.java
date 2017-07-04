package ro.tucn.workload.stream;

import org.apache.log4j.Logger;
import ro.tucn.consumer.AbstractKafkaConsumerCustom;
import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.DurationException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.frame.functions.AssignTimeFunction;
import ro.tucn.logger.SerializableLogger;
import ro.tucn.operator.PairOperator;
import ro.tucn.util.TimeDuration;
import ro.tucn.workload.Workload;
import scala.Tuple2;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Created by Liviu on 4/9/2017.
 */
public class AdvClickStream extends Workload {

    private static final Logger logger = SerializableLogger.getLogger(AdvClickStream.class);

    private final AbstractKafkaConsumerCustom kafkaConsumerCustom;
    private int streamWindowOne;
    private int streamWindowTwo;

    public AdvClickStream(ContextCreator creator) throws WorkloadException {
        super(creator);
        kafkaConsumerCustom = creator.getKafkaConsumerCustom();
        streamWindowOne = Integer.parseInt(properties.getProperty("stream1.window"));
        streamWindowTwo = Integer.parseInt(properties.getProperty("stream2.window"));
    }

    @Override
    public void process() {
        kafkaConsumerCustom.setParallelism(parallelism);
        //System.out.println("3");
        PairOperator<String, String> advs = kafkaConsumerCustom.getStreamPairOperator(properties, TOPIC_ONE_PROPERTY_NAME);
        PairOperator<String, String> clicks = kafkaConsumerCustom.getStreamPairOperator(properties, TOPIC_TWO_PROPERTY_NAME);
        System.out.println("4");
        advs.print();
        clicks.print();
        PairOperator<String, Tuple2<String, String>> advClick = null;
        try {
            System.out.println("5");
            advClick = advs.advClick(clicks,
                    new TimeDuration(TimeUnit.SECONDS, streamWindowOne),
                    new TimeDuration(TimeUnit.SECONDS, streamWindowTwo));
            advClick.print();
            //advClick.sink();
            System.out.println("6");
        } catch (WorkloadException e) {
            logger.error(e.getMessage());
        } catch (DurationException e) {
            logger.error(e.getMessage());
        }
    }

    private static class TimeAssigner implements AssignTimeFunction<Long>, Serializable {
        public long assign(Long var1) {
            return var1;
        }
    }
}