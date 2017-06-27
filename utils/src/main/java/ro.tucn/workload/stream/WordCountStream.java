package ro.tucn.workload.stream;

import org.apache.log4j.Logger;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.kafka.KafkaConsumerCustom;
import ro.tucn.operator.StreamOperator;
import ro.tucn.context.ContextCreator;
import ro.tucn.operator.StreamPairOperator;
import ro.tucn.workload.Workload;

/**
 * Created by Liviu on 4/15/2017.
 */
public class WordCountStream extends Workload {

    private static final Logger logger = Logger.getLogger(WordCountStream.class);
    private final KafkaConsumerCustom kafkaConsumerCustom;

    public WordCountStream(ContextCreator creator) throws WorkloadException {
        super(creator);
        kafkaConsumerCustom = creator.getKafkaConsumerCustom();
    }

    @Override
    public void process() {
        kafkaConsumerCustom.setParallelism(parallelism);
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>WORD COUNT<<<<<<<<<<<<<<<<<");
        StreamOperator<String> words = kafkaConsumerCustom.getStringOperator(properties, "topic1");
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>1<<<<<<<<<<<<<<<<<");
        StreamPairOperator<String, Integer> stringIntegerStreamPairOperator = words.wordCount();
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>2<<<<<<<<<<<<<<<<<");
        stringIntegerStreamPairOperator.print();
    }
}
