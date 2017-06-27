package ro.tucn.flink;

import ro.tucn.exceptions.WorkloadException;
import ro.tucn.flink.operator.FlinkContextCreator;
import ro.tucn.operator.ContextCreator;
import ro.tucn.util.Topics;
import ro.tucn.workload.*;
import ro.tucn.workload.stream.AdvClick;
import ro.tucn.workload.stream.KMeans;
import ro.tucn.workload.stream.WordCount;
import ro.tucn.workload.stream.WordCountFast;

import java.io.IOException;

/**
 * Created by Liviu on 4/16/2017.
 */
public class Application {

    public static void main(String[] args) throws IOException, WorkloadException {
        if (args.length > 0) {
            ContextCreator contextCreator;
            Workload workload = null;
            if (args[0].equalsIgnoreCase(Topics.ADV)) {
                contextCreator = new FlinkContextCreator(Topics.ADV);
                workload = new AdvClick(contextCreator);
            } else if (args[0].equalsIgnoreCase(Topics.K_MEANS)) {
                contextCreator = new FlinkContextCreator(Topics.K_MEANS);
                workload = new KMeans(contextCreator);
            } else if (args[0].equalsIgnoreCase(Topics.UNIFORM_WORDS)) {
                contextCreator = new FlinkContextCreator(Topics.UNIFORM_WORDS);
                workload = new WordCount(contextCreator);
            } else if (args[0].equalsIgnoreCase(Topics.SKEWED_WORDS)) {
                contextCreator = new FlinkContextCreator(Topics.SKEWED_WORDS);
                workload = new WordCountFast(contextCreator);
            } else {
                return;
            }
            workload.Start();
        }
    }
}
