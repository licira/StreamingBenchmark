package ro.tucn.spark;

import ro.tucn.exceptions.WorkloadException;
import ro.tucn.operator.OperatorCreator;
import ro.tucn.spark.operator.SparkOperatorCreator;
import ro.tucn.util.Topics;
import ro.tucn.workload.AdvClick;
import ro.tucn.workload.Workload;

import java.io.IOException;

/**
 * Created by Liviu on 4/7/2017.
 */
public class Application {

    public static void main(String[] args) throws IOException, WorkloadException {
        if (args.length > 0)
        {
            OperatorCreator operatorCreator;
            if (args[0].equalsIgnoreCase(Topics.ADV))
            {
                operatorCreator = new SparkOperatorCreator(Topics.ADV);
                Workload workload = new AdvClick(operatorCreator);
                workload.Start();
            }
        }
    }
}
