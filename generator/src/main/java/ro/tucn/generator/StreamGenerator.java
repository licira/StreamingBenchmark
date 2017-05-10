package ro.tucn.generator;

import org.apache.log4j.Logger;
import ro.tucn.generator.workloadGenerators.*;
import ro.tucn.util.Topics;

/**
 * Created by Liviu on 4/5/2017.
 */
public class StreamGenerator {

    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    public void run(String[] args) {
        int SLEEP_FREQUENCY = 0;
        try {
            SLEEP_FREQUENCY = Integer.parseInt(args[1]);
        } catch (Exception e) {
        }

        Generator generator = null;
        logger.info(args[0]);
        if (args[0].equalsIgnoreCase(Topics.ADV)) {
            generator = new AdvClick();
        } else if (args[0].equalsIgnoreCase(Topics.K_MEANS)) {
            generator = new KMeans();
        } else if (args[0].equalsIgnoreCase(Topics.SKEWED_WORDS)) {
            generator = new SkewedWordCount();
        } else if (args[0].equalsIgnoreCase(Topics.UNIFORM_WORDS)) {
            generator = new UniformWordCount();
        } else {
            return;
        }
        generator.generate(SLEEP_FREQUENCY);
    }
}
