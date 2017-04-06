package ro.tucn.generator;

import org.apache.log4j.Logger;

/**
 * Created by Liviu on 4/5/2017.
 */
public class Application {

    private static final Logger logger = Logger.getLogger("generator");

    public static void main(String args[]) {
        try {
            if (args.length > 0) {
                new StreamGenerator().run(args);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
