package ro.tucn.workload.batch;

import org.apache.log4j.Logger;
import ro.tucn.consumer.AbstractGeneratorConsumer;
import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.operator.BatchOperator;
import ro.tucn.operator.BatchPairOperator;
import ro.tucn.workload.Workload;

/**
 * Created by Liviu on 6/27/2017.
 */
public class WordCountBatch extends Workload {

    private static final Logger logger = Logger.getLogger(WordCountBatch.class);
    private final AbstractGeneratorConsumer generatorConsumer;

    public WordCountBatch(ContextCreator contextCreator) throws WorkloadException {
        super(contextCreator);
        generatorConsumer = contextCreator.getGeneratorConsumer();
    }

    @Override
    public void process() {
        generatorConsumer.setParallelism(parallelism);
        BatchOperator<String> words = generatorConsumer.getStringOperator(properties, "topic1");
        BatchPairOperator<String, Integer> stringIntegerStreamPairOperator = words.wordCount();
        stringIntegerStreamPairOperator.print();
    }
}
