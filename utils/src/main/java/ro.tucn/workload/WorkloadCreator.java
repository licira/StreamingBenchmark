package ro.tucn.workload;

import ro.tucn.DataMode;
import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.topic.ApplicationTopics;
import ro.tucn.workload.batch.AdvClickBatch;
import ro.tucn.workload.batch.KMeansBatch;
import ro.tucn.workload.batch.WordCountBatch;
import ro.tucn.workload.stream.AdvClickStream;
import ro.tucn.workload.stream.KMeansStream;
import ro.tucn.workload.stream.WordCountStream;

/**
 * Created by Liviu on 6/27/2017.
 */
public class WorkloadCreator {

    protected final String NONEXISTING_WORKLOAD_FOR_TOPIC_MSG = "No workload available for this topic.";
    protected final String NONEXISTING_WORKLOAD_FOR_MODE_MSG = "No workload available for this mode.";

    public AbstractWorkload getNewWorkload(ContextCreator contextCreator, String topic, String mode) throws WorkloadException {
        AbstractWorkload workload;
        if (mode.equalsIgnoreCase(DataMode.STREAMING)) {
            workload = getNewStreamWorkload(contextCreator, topic);
        } else if (mode.equalsIgnoreCase(DataMode.BATCH)) {
            workload = getNewBatchWorkload(contextCreator, topic);
        } else {
            throw new RuntimeException(NONEXISTING_WORKLOAD_FOR_MODE_MSG);
        }
        return workload;
    }

    private AbstractWorkload getNewBatchWorkload(ContextCreator contextCreator, String topic) throws WorkloadException {
        AbstractWorkload workload;
        if (topic.equalsIgnoreCase(ApplicationTopics.ADV)) {
            workload = new AdvClickBatch(contextCreator);
        } else if (topic.equalsIgnoreCase(ApplicationTopics.K_MEANS)) {
            workload = new KMeansBatch(contextCreator);
        } else if ((topic.equalsIgnoreCase(ApplicationTopics.SKEWED_WORDS)) ||
                (topic.equalsIgnoreCase(ApplicationTopics.UNIFORM_WORDS))) {
            workload = new WordCountBatch(contextCreator);
        } else {
            throw new RuntimeException(NONEXISTING_WORKLOAD_FOR_TOPIC_MSG);
        }
        return workload;
    }

    private AbstractWorkload getNewStreamWorkload(ContextCreator contextCreator, String topic) throws WorkloadException {
        AbstractWorkload workload;
        if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.ADV))) {
            workload = new AdvClickStream(contextCreator);
        } else if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.K_MEANS))) {
            workload = new KMeansStream(contextCreator);
        } else if ((topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.SKEWED_WORDS))) ||
                (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.UNIFORM_WORDS)))) {
            workload = new WordCountStream(contextCreator);
        } else {
            throw new RuntimeException(NONEXISTING_WORKLOAD_FOR_TOPIC_MSG);
        }
        return workload;
    }
}
