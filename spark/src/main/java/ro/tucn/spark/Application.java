package ro.tucn.spark;

import ro.tucn.exceptions.WorkloadException;
import ro.tucn.operator.OperatorCreator;
import ro.tucn.spark.operator.SparkOperatorCreator;
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
            OperatorCreator operatorCreator;
            Workload workload = null;
            if (args[0].equalsIgnoreCase(Topics.ADV))
            {
                operatorCreator = new SparkOperatorCreator(Topics.ADV);
                workload = new AdvClick(operatorCreator);
            } else if (args[0].equalsIgnoreCase(Topics.K_MEANS)) {
                operatorCreator = new SparkOperatorCreator(Topics.K_MEANS);
                workload = new KMeans(operatorCreator);
            } else if (args[0].equalsIgnoreCase(Topics.UNIFORM_WORDS)) {
                operatorCreator = new SparkOperatorCreator(Topics.UNIFORM_WORDS);
                workload = new WordCount(operatorCreator);
            } else if (args[0].equalsIgnoreCase(Topics.SKEWED_WORDS)) {
                operatorCreator = new SparkOperatorCreator(Topics.SKEWED_WORDS);
                workload = new WordCountFast(operatorCreator);
            } else {
                return;
            }
            workload.Start();
        }
    }
}
