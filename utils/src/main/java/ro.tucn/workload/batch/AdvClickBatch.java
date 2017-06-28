package ro.tucn.workload.batch;

import org.apache.log4j.Logger;
import ro.tucn.consumer.AbstractGeneratorConsumer;
import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.DurationException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.operator.PairOperator;
import ro.tucn.util.TimeDuration;
import ro.tucn.workload.Workload;
import scala.Tuple2;

import java.util.concurrent.TimeUnit;

/**
 * Created by Liviu on 6/27/2017.
 */
public class AdvClickBatch extends Workload {

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
        PairOperator<String, String> advs = generatorConsumer.getPairOperator(properties, "topic1");
        PairOperator<String, String> clicks = generatorConsumer.getPairOperator(properties, "topic2");
        advs.print();
        clicks.print();
        PairOperator<String, Tuple2<String, String>> advClick = null;
        try {
            System.out.println("5");
            advClick = advs.join(clicks,
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
}
