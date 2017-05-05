package ro.tucn.generator.helper;

import org.apache.log4j.Logger;

/**
 * Created by Liviu on 5/6/2017.
 */
public class TimeHelper {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    public static void temporizeDataGeneration(int sleepFrequency, long step) {
        // control data generate speed
        if (sleepFrequency > 0 && step % sleepFrequency == 0) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

    public static long getNanoTime() {
        return System.nanoTime();
    }
}
