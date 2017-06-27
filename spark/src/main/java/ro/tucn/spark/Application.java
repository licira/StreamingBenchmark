package ro.tucn.spark;

import ro.tucn.exceptions.WorkloadException;
import ro.tucn.operator.ContextCreator;
import ro.tucn.spark.context.SparkContextCreator;
import ro.tucn.topic.ApplicationTopics;
import ro.tucn.util.ArgsParser;
import ro.tucn.workload.Workload;
import ro.tucn.workload.stream.AdvClickStream;
import ro.tucn.workload.stream.KMeansStream;
import ro.tucn.workload.stream.WordCountFastStream;
import ro.tucn.workload.stream.WordCountStream;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by Liviu on 4/7/2017.
 */
public class Application {

    public static void main(String[] args) throws IOException, WorkloadException {
        if (args.length > 0)
        {
            HashMap<String, String> parsedArgs = ArgsParser.parseArgs(args);
            String topic = ArgsParser.getTopic(parsedArgs);
            ContextCreator contextCreator;
            Workload workload = null;
            if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.ADV))) {
                contextCreator = new SparkContextCreator(String.valueOf(ApplicationTopics.ADV));
                workload = new AdvClickStream(contextCreator);
            } else if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.K_MEANS))) {
                contextCreator = new SparkContextCreator(String.valueOf(ApplicationTopics.K_MEANS));
                workload = new KMeansStream(contextCreator);
            } else if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.UNIFORM_WORDS))) {
                contextCreator = new SparkContextCreator(String.valueOf(ApplicationTopics.UNIFORM_WORDS));
                workload = new WordCountStream(contextCreator);
            } else if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.SKEWED_WORDS))) {
                contextCreator = new SparkContextCreator(String.valueOf(ApplicationTopics.SKEWED_WORDS));
                workload = new WordCountFastStream(contextCreator);
            } else {
                return;
            }
            workload.Start();
        }
    }
}
