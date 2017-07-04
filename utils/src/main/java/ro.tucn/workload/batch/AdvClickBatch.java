package ro.tucn.workload.batch;

import org.apache.log4j.Logger;
import ro.tucn.consumer.AbstractGeneratorConsumer;
import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.DurationException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.operator.PairOperator;
import ro.tucn.topic.ApplicationTopics;
import ro.tucn.util.TimeDuration;
import ro.tucn.workload.AbstractWorkload;
import scala.Tuple2;

import java.util.concurrent.TimeUnit;

/**
 * Created by Liviu on 6/27/2017.
 */
public class AdvClickBatch extends AbstractWorkload {

    private static final Logger logger = Logger.getLogger(AdvClickBatch.class);
    private final AbstractGeneratorConsumer generatorConsumer;
    private int streamWindowOne;
    private int streamWindowTwo;

    public AdvClickBatch(ContextCreator contextCreator) throws WorkloadException {
        super(contextCreator);
        generatorConsumer = contextCreator.getGeneratorConsumer();
        streamWindowOne = Integer.parseInt(properties.getProperty("stream1.window"));
        streamWindowTwo = Integer.parseInt(properties.getProperty("stream2.window"));
    }

    @Override
    public void process()  {
        generatorConsumer.setParallelism(parallelism);
        generatorConsumer.askGeneratorToProduceData(ApplicationTopics.ADV);
        PairOperator<String, String> advs = generatorConsumer.getPairOperator(properties, TOPIC_ONE_PROPERTY_NAME);
        PairOperator<String, String> clicks = generatorConsumer.getPairOperator(properties, TOPIC_TWO_PROPERTY_NAME);
        advs.print();
        clicks.print();
        PairOperator<String, Tuple2<String, String>> advClick = null;
        try {
            advClick = advs.advClick(clicks,
                    new TimeDuration(TimeUnit.SECONDS, streamWindowOne),
                    new TimeDuration(TimeUnit.SECONDS, streamWindowTwo));
            advClick.print();
        } catch (WorkloadException e) {
            logger.error(e.getMessage());
        } catch (DurationException e) {
            logger.error(e.getMessage());
        }
    }
}
