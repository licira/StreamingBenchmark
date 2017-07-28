package ro.tucn.workload.batch;

import org.apache.log4j.Logger;
import ro.tucn.consumer.AbstractGeneratorConsumer;
import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.operator.BatchPairOperator;
import ro.tucn.topic.ApplicationTopics;
import ro.tucn.workload.AbstractAdvClick;

/**
 * Created by Liviu on 6/27/2017.
 */
public class AdvClickBatch extends AbstractAdvClick {

    private static final Logger logger = Logger.getLogger(AdvClickBatch.class);
    private final AbstractGeneratorConsumer generatorConsumer;
    private int streamWindowOne;
    private int streamWindowTwo;

    public AdvClickBatch(ContextCreator contextCreator) throws WorkloadException {
        super(contextCreator);
        streamWindowOne = Integer.parseInt(properties.getProperty("stream1.window"));
        streamWindowTwo = Integer.parseInt(properties.getProperty("stream2.window"));
        generatorConsumer = contextCreator.getGeneratorConsumer();
    }

    @Override
    public void process()  {
        generatorConsumer.askGeneratorToProduceData(ApplicationTopics.ADV, numberOfEntities);
        BatchPairOperator<String, String> advs = generatorConsumer.getPairOperator(properties, TOPIC_ONE_PROPERTY_NAME);
        BatchPairOperator<String, String> clicks = generatorConsumer.getPairOperator(properties, TOPIC_TWO_PROPERTY_NAME);
        super.process(advs, clicks, streamWindowOne, streamWindowTwo);
    }
}
