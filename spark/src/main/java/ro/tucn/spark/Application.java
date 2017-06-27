package ro.tucn.spark;

import ro.tucn.exceptions.WorkloadException;
import ro.tucn.context.ContextCreator;
import ro.tucn.spark.context.SparkContextCreator;
import ro.tucn.util.ArgsParser;
import ro.tucn.workload.Workload;
import ro.tucn.workload.WorkloadCreator;

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
            ContextCreator contextCreator = new SparkContextCreator(topic);
            WorkloadCreator workloadCreator = new WorkloadCreator();
            Workload workload = workloadCreator.getNewWorkload(contextCreator, topic);
            workload.Start();
        }
    }
}
