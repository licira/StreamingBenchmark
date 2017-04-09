package ro.tucn.statistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.tucn.util.Configuration;

/**
 * Created by Liviu on 4/8/2017.
 */
public class CentroidLog {

    private static Logger logger = LoggerFactory.getLogger(CentroidLog.class);

    public void execute(long counts, double[] location) {
        double probability = 0.05;
        if (Configuration.kmeansCentroidsFrequency != null
                && Configuration.kmeansCentroidsFrequency > 0) {
            probability = Configuration.kmeansCentroidsFrequency;
        }

        if (Math.random() < probability) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("\t%d", counts));
            for (double d : location) {
                sb.append(String.format("\t%16.14f", d));
            }
            logger.warn(sb.toString());
        }
    }
}