package ro.tucn.generator;

import org.apache.log4j.Logger;
import ro.tucn.logger.SerializableLogger;
import ro.tucn.generator.workloadGenerators.AdvClick;
import ro.tucn.generator.workloadGenerators.KMeansPoints;
import ro.tucn.generator.workloadGenerators.SkewedWordCount;
import ro.tucn.generator.workloadGenerators.UniformWordCount;
import ro.tucn.util.Topics;

/**
 * Created by Liviu on 4/5/2017.
 */
public class StreamGenerator {

    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    public void run(String[] args) throws Exception {
        int SLEEP_FREQUENCY = 0;
        try {
            SLEEP_FREQUENCY = Integer.parseInt(args[1]);
        } catch (Exception e) {
        }

        if (args[0].equalsIgnoreCase(Topics.ADV)) {
            logger.info(args[0]);
            new AdvClick().generate(SLEEP_FREQUENCY);
        } else if (args[0].equalsIgnoreCase(Topics.K_MEANS)) {
            logger.info(args[0]);
            new KMeansPoints().generate(SLEEP_FREQUENCY);
        } else if (args[0].equalsIgnoreCase(Topics.SKEWED_WORDS)) {
            logger.info(args[0]);
            new SkewedWordCount().generate(SLEEP_FREQUENCY);
        } else if (args[0].equalsIgnoreCase(Topics.UNIFORM_WORDS)) {
            logger.info(args[0]);
            new UniformWordCount().generate(SLEEP_FREQUENCY);
        }
    }
}
