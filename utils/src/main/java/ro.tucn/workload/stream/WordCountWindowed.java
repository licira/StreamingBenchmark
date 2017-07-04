package ro.tucn.workload.stream;

import org.apache.log4j.Logger;
import ro.tucn.consumer.AbstractKafkaConsumerCustom;
import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.workload.AbstractWorkload;

/**
 * Created by Liviu on 4/15/2017.
 */
public class WordCountWindowed extends AbstractWorkload {

    private static final Logger logger = Logger.getLogger(WordCountWindowed.class);
    private final AbstractKafkaConsumerCustom kafkaConsumerCustom;

    public WordCountWindowed(ContextCreator creator) throws WorkloadException {
        super(creator);
        kafkaConsumerCustom = creator.getKafkaConsumerCustom();
    }

    @Override
    public void process() {
        try {
            // Flink doesn't support shuffle().window()
            // Actually Flink does keyGrouping().window().update()
            // It is the same situation to Spark streaming
            /*Operator<TimeHolder<String>> wordOperators = kafkaConsumerCustom.getStringStreamTimeHolderOperator(properties, TOPIC_ONE_PROPERTY_NAME);
            StreamPairOperator<String, TimeHolder<Integer>> counts =
                    wordOperators.flatMap(UserFunctions.splitFlatMapTimeHolder, "splitter")
                            .mapToPair(UserFunctions.mapToStrIntPairTimeHolder, "pair")
                            .reduceByKeyAndWindow(UserFunctions.sumReduceTimeHolder2, "counter",
                                    new TimeDuration(TimeUnit.SECONDS, 1), new TimeDuration(TimeUnit.SECONDS, 1));
            counts.sink();*/
            //cumulate counts
            //StreamPairOperator<String, Integer> cumulateCounts = counts.updateStateByKey(UserFunctions.sumReduce, "cumulate");
            //cumulateCounts.print();
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
