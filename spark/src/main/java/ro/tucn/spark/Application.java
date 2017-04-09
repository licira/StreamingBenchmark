package ro.tucn.spark;

import ro.tucn.operator.OperatorCreator;
import ro.tucn.spark.operator.SparkOperatorCreator;
import ro.tucn.util.Topics;

import java.io.IOException;

/**
 * Created by Liviu on 4/7/2017.
 */
public class Application {

    public static void main(String[] args) throws IOException {
        if (args.length > 0) {
            OperatorCreator operatorCreator;
            if (args[0].equalsIgnoreCase(Topics.ADV)) {
                operatorCreator = new SparkOperatorCreator(Topics.ADV);
            }

        }
    }
}
