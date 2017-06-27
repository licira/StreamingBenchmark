package ro.tucn.workload.stream;

import org.apache.log4j.Logger;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.kafka.KafkaConsumerCustom;
import ro.tucn.operator.StreamOperator;
import ro.tucn.operator.ContextCreator;
import ro.tucn.operator.StreamPairOperator;
import ro.tucn.workload.Workload;

/**
 * Created by Liviu on 4/15/2017.
 */
public class WordCountFast extends Workload {

    private static final Logger logger = Logger.getLogger(WordCount.class);
    private final KafkaConsumerCustom kafkaConsumerCustom;

    public WordCountFast(ContextCreator creator) throws WorkloadException {
        super(creator);
        kafkaConsumerCustom = creator.getKafkaConsumerCustom();
    }

    @Override
    public void process() {
        kafkaConsumerCustom.setParallelism(parallelism);
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>WORD COUNT FAST<<<<<<<<<<<<<<<<<");
        StreamOperator<String> wordOperators = kafkaConsumerCustom.getStringOperator(properties, "topic1");
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>1<<<<<<<<<<<<<<<<<");
        wordOperators.print();
        StreamPairOperator<String, Integer> stringIntegerStreamPairOperator = wordOperators.wordCount();
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>2<<<<<<<<<<<<<<<<<");
        stringIntegerStreamPairOperator.print();
    }
}
