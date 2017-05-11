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

        AbstractGenerator generator = null;
        logger.info(args[0]);
        if (args[0].equalsIgnoreCase(Topics.ADV)) {
            generator = new AdvClickGenerator();
        } else if (args[0].equalsIgnoreCase(Topics.K_MEANS)) {
            generator = new KMeansGenerator();
        } else if (args[0].equalsIgnoreCase(Topics.SKEWED_WORDS)) {
            generator = new SkewedWordsGenerator();
        } else if (args[0].equalsIgnoreCase(Topics.UNIFORM_WORDS)) {
            generator = new UniformWordsGenerator();
        } else {
            return;
        }
        generator.generate(SLEEP_FREQUENCY);
    }
}
