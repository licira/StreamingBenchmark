package ro.tucn.flink;

import ro.tucn.exceptions.WorkloadException;
import ro.tucn.flink.operator.FlinkContextCreator;
import ro.tucn.operator.ContextCreator;
import ro.tucn.util.ArgsParser;
import ro.tucn.util.Topics;
import ro.tucn.workload.*;
import ro.tucn.workload.stream.AdvClickStream;
import ro.tucn.workload.stream.KMeansStream;
import ro.tucn.workload.stream.WordCountStream;
import ro.tucn.workload.stream.WordCountFastStream;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by Liviu on 4/16/2017.
 */
public class Application {

    public static void main(String[] args) throws IOException, WorkloadException {
        if (args.length > 0) {
            HashMap<String, String> parsedArgs = ArgsParser.parseArgs(args);
            String topic = ArgsParser.getTopic(parsedArgs);
            ContextCreator contextCreator;
            Workload workload = null;
            if (topic.equalsIgnoreCase(Topics.ADV)) {
                contextCreator = new FlinkContextCreator(Topics.ADV);
                workload = new AdvClickStream(contextCreator);
            } else if (topic.equalsIgnoreCase(Topics.K_MEANS)) {
                contextCreator = new FlinkContextCreator(Topics.K_MEANS);
                workload = new KMeansStream(contextCreator);
            } else if (topic.equalsIgnoreCase(Topics.UNIFORM_WORDS)) {
                contextCreator = new FlinkContextCreator(Topics.UNIFORM_WORDS);
                workload = new WordCountStream(contextCreator);
            } else if (topic.equalsIgnoreCase(Topics.SKEWED_WORDS)) {
                contextCreator = new FlinkContextCreator(Topics.SKEWED_WORDS);
                workload = new WordCountFastStream(contextCreator);
            } else {
                return;
            }
            workload.Start();
        }
    }
}
