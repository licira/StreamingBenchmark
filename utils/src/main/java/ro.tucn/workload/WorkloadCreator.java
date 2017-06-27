package ro.tucn.workload;

import ro.tucn.exceptions.WorkloadException;
import ro.tucn.context.ContextCreator;
import ro.tucn.topic.ApplicationTopics;
import ro.tucn.workload.stream.AdvClickStream;
import ro.tucn.workload.stream.KMeansStream;
import ro.tucn.workload.stream.WordCountFastStream;
import ro.tucn.workload.stream.WordCountStream;

/**
 * Created by Liviu on 6/27/2017.
 */
public class WorkloadCreator {

    protected final String NONEXISTING_WORKLOAD_EXCEPTION_MSG = "No workload available for this.";

    public Workload getNewWorkload(ContextCreator contextCreator, String topic) throws WorkloadException{
        Workload workload;
        if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.ADV))) {
            workload = new AdvClickStream(contextCreator);
        } else if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.K_MEANS))) {
            workload = new KMeansStream(contextCreator);
        } else if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.SKEWED_WORDS))) {
            workload = new WordCountStream(contextCreator);
        } else if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.UNIFORM_WORDS))) {
            workload = new WordCountFastStream(contextCreator);
        } else {
            throw new RuntimeException(NONEXISTING_WORKLOAD_EXCEPTION_MSG);
        }
        return workload;
    }
}
