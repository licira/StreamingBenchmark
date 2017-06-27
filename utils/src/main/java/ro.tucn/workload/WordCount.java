package ro.tucn.workload;

import org.apache.log4j.Logger;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.kafka.KafkaConsumerCustom;
import ro.tucn.operator.Operator;
import ro.tucn.operator.ContextCreator;
import ro.tucn.operator.PairOperator;

/**
 * Created by Liviu on 4/15/2017.
 */
public class WordCount extends Workload {

    private static final Logger logger = Logger.getLogger(WordCount.class);
    private final KafkaConsumerCustom kafkaConsumerCustom;

    public WordCount(ContextCreator creator) throws WorkloadException {
        super(creator);
        kafkaConsumerCustom = creator.getKafkaConsumerCustom();
    }

    @Override
    public void process() {
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>WORD COUNT<<<<<<<<<<<<<<<<<");
        Operator<String> words = kafkaConsumerCustom.getStringOperator(properties, "topic1");
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>1<<<<<<<<<<<<<<<<<");
        PairOperator<String, Integer> stringIntegerPairOperator = words.wordCount();
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>2<<<<<<<<<<<<<<<<<");
        stringIntegerPairOperator.print();
    }
}
