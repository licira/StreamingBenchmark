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
public class WordCountStream extends Workload {

    private static final Logger logger = Logger.getLogger(WordCountStream.class);
    private final AbstractKafkaConsumerCustom kafkaConsumerCustom;

    public WordCountStream(ContextCreator creator) throws WorkloadException {
        super(creator);
        kafkaConsumerCustom = creator.getKafkaConsumerCustom();
    }

    @Override
    public void process() {
        kafkaConsumerCustom.setParallelism(parallelism);
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>WORD COUNT<<<<<<<<<<<<<<<<<");
        StreamOperator<String> words = kafkaConsumerCustom.getStringOperator(properties, TOPIC_ONE_PROPERTY_NAME);
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>1<<<<<<<<<<<<<<<<<");
        StreamPairOperator<String, Integer> countedWords = words.wordCount();
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>2<<<<<<<<<<<<<<<<<");
        countedWords.print();
    }
}
