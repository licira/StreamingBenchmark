package ro.tucn.spark;

import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.spark.context.SparkContextCreator;
import ro.tucn.util.ArgsParser;
import ro.tucn.workload.AbstractWorkload;
import ro.tucn.workload.WorkloadCreator;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by Liviu on 4/7/2017.
 */
public class Application {

    public static void main(String[] args) throws IOException, WorkloadException {
        if (args.length > 0) {
            HashMap<String, String> parsedArgs = ArgsParser.parseArgs(args);
            ArgsParser.checkParamsValidityForTestBed(parsedArgs);
            String topic = ArgsParser.getTopic(parsedArgs);
            String mode = ArgsParser.getMode(parsedArgs);
            ContextCreator contextCreator = new SparkContextCreator(topic, mode);
            WorkloadCreator workloadCreator = new WorkloadCreator();
            AbstractWorkload workload = workloadCreator.getNewWorkload(contextCreator, topic, mode);
            workload.Start();
        }
    }
}
