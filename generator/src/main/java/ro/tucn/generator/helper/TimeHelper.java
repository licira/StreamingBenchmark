package ro.tucn.generator.helper;

import org.apache.log4j.Logger;

/**
 * Created by Liviu on 5/6/2017.
 */
public class TimeHelper {

    protected static final Logger logger = Logger.getLogger(TimeHelper.class.getSimpleName());

    public static void temporizeDataGeneration(int sleepFrequency, long sleepDuration, long step) {
        // control data generate speed
        if ((sleepFrequency > 0) && (step % sleepFrequency == 0)) {
            try {
                Thread.sleep(sleepDuration);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

    public static long getNanoTime() {
        return System.nanoTime();
    }
}
