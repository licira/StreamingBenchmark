package ro.tucn.generator;

import org.apache.log4j.Logger;

/**
 * Created by Liviu on 4/5/2017.
 */
public class StreamGenerator {

    private final Logger logger = Logger.getLogger(this.getClass());

    public void run(String[] args) throws Exception{
        int SLEEP_FREQUENCY = 0;
        try {
            SLEEP_FREQUENCY = Integer.parseInt(args[1]);
        } catch (Exception e) {
        }

        if (args[0].equalsIgnoreCase("adv")) {
            new AdvClick().generate(SLEEP_FREQUENCY);
        } else if (args[0].equalsIgnoreCase("kmeans")) {
            new KMeansPoints().generate(SLEEP_FREQUENCY);
        } else if (args[0].equalsIgnoreCase("skewed-words")) {
            new SkewedWordCount().generate(SLEEP_FREQUENCY);
        } else if (args[0].equalsIgnoreCase("uniform-word-count")) {
            new UniformWordCount().generate(SLEEP_FREQUENCY);
        }
    }
}
