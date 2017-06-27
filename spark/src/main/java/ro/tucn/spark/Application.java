package ro.tucn.spark;

import ro.tucn.exceptions.WorkloadException;
import ro.tucn.operator.ContextCreator;
import ro.tucn.spark.operator.SparkContextCreator;
import ro.tucn.util.Topics;
import ro.tucn.workload.*;

import java.io.IOException;

/**
 * Created by Liviu on 4/7/2017.
 */
public class Application {

    public static void main(String[] args) throws IOException, WorkloadException {
        if (args.length > 0)
        {
            ContextCreator ContextCreator;
            Workload workload = null;
            if (args[0].equalsIgnoreCase(Topics.ADV))
            {
                ContextCreator = new SparkContextCreator(Topics.ADV);
                workload = new AdvClick(ContextCreator);
            } else if (args[0].equalsIgnoreCase(Topics.K_MEANS)) {
                ContextCreator = new SparkContextCreator(Topics.K_MEANS);
                workload = new KMeans(ContextCreator);
            } else if (args[0].equalsIgnoreCase(Topics.UNIFORM_WORDS)) {
                ContextCreator = new SparkContextCreator(Topics.UNIFORM_WORDS);
                workload = new WordCount(ContextCreator);
            } else if (args[0].equalsIgnoreCase(Topics.SKEWED_WORDS)) {
                ContextCreator = new SparkContextCreator(Topics.SKEWED_WORDS);
                workload = new WordCountFast(ContextCreator);
            } else {
                return;
            }
            workload.Start();
        }
    }
}
