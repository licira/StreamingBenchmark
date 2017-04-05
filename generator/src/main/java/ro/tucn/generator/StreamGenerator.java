package ro.tucn.generator;

import org.apache.log4j.Logger;

/**
 * Created by Liviu on 4/5/2017.
 */
public class StreamGenerator {

    private final Logger logger = Logger.getLogger(this.getClass());

    public void run(String[] args) throws InterruptedException {
        if(args[0].equals("adv"))
        {
            int SLEEP_FREQUENCY;
            try {
                 SLEEP_FREQUENCY = Integer.parseInt(args[1]);
            } catch (Exception e) {
                SLEEP_FREQUENCY = 0;
            }
            new AdvClick().generate(SLEEP_FREQUENCY);
        }
    }
}
