package ro.tucn.workload.stream;

import org.apache.log4j.Logger;
import ro.tucn.consumer.AbstractKafkaConsumerCustom;
import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.operator.StreamOperator;
import ro.tucn.workload.AbstractWordCount;

/**
 * Created by Liviu on 4/15/2017.
 */
public class WordCountStream extends AbstractWordCount {

    private static final Logger logger = Logger.getLogger(WordCountStream.class);
    private final AbstractKafkaConsumerCustom kafkaConsumerCustom;

    public WordCountStream(ContextCreator creator) throws WorkloadException {
        super(creator);
        kafkaConsumerCustom = creator.getKafkaConsumerCustom();
        kafkaConsumerCustom.setParallelism(parallelism);
    }

    @Override
    public void process() {
        StreamOperator<String> words = kafkaConsumerCustom.getStringOperator(properties, TOPIC_ONE_PROPERTY_NAME);
        super.process(words);
    }
}
