package ro.tucn.workload;

import org.apache.log4j.Logger;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.kafka.KafkaConsumerCustom;
import ro.tucn.operator.Operator;
import ro.tucn.operator.OperatorCreator;
import ro.tucn.operator.PairOperator;

/**
 * Created by Liviu on 4/15/2017.
 */
public class WordCountFast extends Workload {

    private static final Logger logger = Logger.getLogger(WordCount.class);
    private final KafkaConsumerCustom kafkaConsumerCustom;

    public WordCountFast(OperatorCreator creator) throws WorkloadException {
        super(creator);
        kafkaConsumerCustom = creator.getKafkaConsumerCustom();
    }

    @Override
    public void process() {
        kafkaConsumerCustom.setParallelism(parallelism);
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>WORD COUNT FAST<<<<<<<<<<<<<<<<<");
        Operator<String> wordOperators = kafkaConsumerCustom.getStringOperator(properties, "topic1");
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>1<<<<<<<<<<<<<<<<<");
        wordOperators.print();
        PairOperator<String, Integer> stringIntegerPairOperator = wordOperators.wordCount();
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>2<<<<<<<<<<<<<<<<<");
        stringIntegerPairOperator.print();
    }
}
