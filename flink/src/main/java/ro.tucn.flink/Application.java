package ro.tucn.flink;

import ro.tucn.exceptions.WorkloadException;
import ro.tucn.flink.operator.FlinkOperatorCreator;
import ro.tucn.operator.OperatorCreator;
import ro.tucn.util.Topics;
import ro.tucn.workload.AdvClick;
import ro.tucn.workload.KMeans;
import ro.tucn.workload.Workload;

import java.io.IOException;

/**
 * Created by Liviu on 4/16/2017.
 */
public class Application {

    public static void main(String[] args) throws IOException, WorkloadException {
        if (args.length > 0) {
            OperatorCreator operatorCreator;
            Workload workload = null;
            if (args[0].equalsIgnoreCase(Topics.ADV)) {
                operatorCreator = new FlinkOperatorCreator(Topics.ADV);
                workload = new AdvClick(operatorCreator);
            } else if (args[0].equalsIgnoreCase(Topics.K_MEANS)) {
                operatorCreator = new FlinkOperatorCreator(Topics.K_MEANS);
                workload = new KMeans(operatorCreator);
            } else if (args[0].equalsIgnoreCase(Topics.UNIFORM_WORDS)) {
                operatorCreator = new FlinkOperatorCreator(Topics.UNIFORM_WORDS);
                workload = new AdvClick(operatorCreator);
            } else if (args[0].equalsIgnoreCase(Topics.SKEWED_WORDS)) {
                operatorCreator = new FlinkOperatorCreator(Topics.SKEWED_WORDS);
                workload = new AdvClick(operatorCreator);
            } else {
                return;
            }
            workload.Start();
        }
    }
}
