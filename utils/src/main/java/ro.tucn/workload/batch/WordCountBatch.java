package ro.tucn.workload.batch;

import org.apache.log4j.Logger;
import ro.tucn.consumer.AbstractGeneratorConsumer;
import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.operator.BatchOperator;
import ro.tucn.operator.BatchPairOperator;
import ro.tucn.topic.ApplicationTopics;
import ro.tucn.workload.AbstractWorkload;

/**
 * Created by Liviu on 6/27/2017.
 */
public class WordCountBatch extends AbstractWorkload {

    private static final Logger logger = Logger.getLogger(WordCountBatch.class);
    private final AbstractGeneratorConsumer generatorConsumer;

    public WordCountBatch(ContextCreator contextCreator) throws WorkloadException {
        super(contextCreator);
        generatorConsumer = contextCreator.getGeneratorConsumer();
        generatorConsumer.setParallelism(parallelism);
        generatorConsumer.askGeneratorToProduceData(ApplicationTopics.SKEWED_WORDS);
    }

    @Override
    public void process() {
        BatchOperator<String> words = generatorConsumer.getStringOperator(properties, TOPIC_ONE_PROPERTY_NAME);
        BatchPairOperator<String, Integer> countedWords = words.wordCount();
        countedWords.print();
    }
}
