package ro.tucn.workload.stream;

import org.apache.log4j.Logger;
import ro.tucn.consumer.AbstractKafkaConsumerCustom;
import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.logger.SerializableLogger;
import ro.tucn.operator.StreamPairOperator;
import ro.tucn.workload.AbstractAdvClick;

/**
 * Created by Liviu on 4/9/2017.
 */
public class AdvClickStream extends AbstractAdvClick {

    private static final Logger logger = SerializableLogger.getLogger(AdvClickStream.class);

    private final AbstractKafkaConsumerCustom kafkaConsumerCustom;
    private int streamWindowOne;
    private int streamWindowTwo;
    private Object performanceListener;

    public AdvClickStream(ContextCreator creator) throws WorkloadException {
        super(creator);
        performanceListener = creator.getPerformanceListener();
        streamWindowOne = Integer.parseInt(properties.getProperty("stream1.window"));
        streamWindowTwo = Integer.parseInt(properties.getProperty("stream2.window"));
        kafkaConsumerCustom = creator.getKafkaConsumerCustom();
        kafkaConsumerCustom.setParallelism(parallelism);
    }

    @Override
    public void process() {
        StreamPairOperator<String, String> advs = kafkaConsumerCustom.getStreamPairOperator(properties, TOPIC_ONE_PROPERTY_NAME);
        StreamPairOperator<String, String> clicks = kafkaConsumerCustom.getStreamPairOperator(properties, TOPIC_TWO_PROPERTY_NAME);
        advs.setPerformanceListener(performanceListener);
        super.process(advs, clicks, streamWindowOne, streamWindowTwo);
    }
}