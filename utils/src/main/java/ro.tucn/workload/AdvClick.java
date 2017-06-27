package ro.tucn.workload;

import org.apache.log4j.Logger;
import ro.tucn.exceptions.DurationException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.frame.functions.AssignTimeFunction;
import ro.tucn.kafka.KafkaConsumerCustom;
import ro.tucn.logger.SerializableLogger;
import ro.tucn.operator.ContextCreator;
import ro.tucn.operator.PairOperator;
import ro.tucn.util.TimeDuration;
import scala.Tuple2;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Created by Liviu on 4/9/2017.
 */
public class AdvClick extends Workload {

    private static final Logger logger = SerializableLogger.getLogger(AdvClick.class);

    private final KafkaConsumerCustom kafkaConsumerCustom;
    private int streamWindowOne;
    private int streamWindowTwo;

    public AdvClick(ContextCreator creator) throws WorkloadException {
        super(creator);
        kafkaConsumerCustom = creator.getKafkaConsumerCustom();
        streamWindowOne = Integer.parseInt(properties.getProperty("stream1.window"));
        streamWindowTwo = Integer.parseInt(properties.getProperty("stream2.window"));
    }

    @Override
    public void process() {
        kafkaConsumerCustom.setParallelism(parallelism);
        //System.out.println("3");
        //BatchOperator<String> advs = kafkaConsumerCustom.getBatchStringOperator(properties, "topic1");
        //BatchOperator<String> clicks = kafkaConsumerCustom.getBatchStringOperator(properties, "topic2");
        PairOperator<String, String> advs = kafkaConsumerCustom.getStreamPairOperator(properties, "topic1");
        PairOperator<String, String> clicks = kafkaConsumerCustom.getStreamPairOperator(properties, "topic2");
        System.out.println("4");
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

    private static class TimeAssigner implements AssignTimeFunction<Long>, Serializable {
        public long assign(Long var1) {
            return var1;
        }
    }
}