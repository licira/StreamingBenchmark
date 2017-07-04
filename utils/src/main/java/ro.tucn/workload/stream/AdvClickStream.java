package ro.tucn.workload.stream;

import org.apache.log4j.Logger;
import ro.tucn.consumer.AbstractKafkaConsumerCustom;
import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.DurationException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.logger.SerializableLogger;
import ro.tucn.operator.PairOperator;
import ro.tucn.util.TimeDuration;
import ro.tucn.workload.AbstractWorkload;
import scala.Tuple2;

import java.util.concurrent.TimeUnit;

/**
 * Created by Liviu on 4/9/2017.
 */
public class AdvClickStream extends AbstractWorkload {

    private static final Logger logger = SerializableLogger.getLogger(AdvClickStream.class);

    private final AbstractKafkaConsumerCustom kafkaConsumerCustom;
    private int streamWindowOne;
    private int streamWindowTwo;

    public AdvClickStream(ContextCreator creator) throws WorkloadException {
        super(creator);
        streamWindowOne = Integer.parseInt(properties.getProperty("stream1.window"));
        streamWindowTwo = Integer.parseInt(properties.getProperty("stream2.window"));
        kafkaConsumerCustom = creator.getKafkaConsumerCustom();
        kafkaConsumerCustom.setParallelism(parallelism);
    }

    @Override
    public void process() {
        PairOperator<String, String> advs = kafkaConsumerCustom.getStreamPairOperator(properties, TOPIC_ONE_PROPERTY_NAME);
        PairOperator<String, String> clicks = kafkaConsumerCustom.getStreamPairOperator(properties, TOPIC_TWO_PROPERTY_NAME);
        advs.print();
        clicks.print();
        PairOperator<String, Tuple2<String, String>> advClick = null;
        try {
            advClick = advs.advClick(clicks,
                    new TimeDuration(TimeUnit.SECONDS, streamWindowOne),
                    new TimeDuration(TimeUnit.SECONDS, streamWindowTwo));
            advClick.print();
            //advClick.sink();
        } catch (WorkloadException e) {
            logger.error(e.getMessage());
        } catch (DurationException e) {
            logger.error(e.getMessage());
        }
    }
}