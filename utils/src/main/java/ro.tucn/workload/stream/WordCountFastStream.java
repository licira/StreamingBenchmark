package ro.tucn.workload.stream;

import org.apache.log4j.Logger;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.consumer.AbstractKafkaConsumerCustom;
import ro.tucn.operator.StreamOperator;
import ro.tucn.context.ContextCreator;
import ro.tucn.operator.StreamPairOperator;
import ro.tucn.workload.Workload;

/**
 * Created by Liviu on 4/15/2017.
 */
public class WordCountFastStream extends Workload {

    private static final Logger logger = Logger.getLogger(WordCountFastStream.class);
    private final AbstractKafkaConsumerCustom kafkaConsumerCustom;

    public WordCountFastStream(ContextCreator creator) throws WorkloadException {
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
