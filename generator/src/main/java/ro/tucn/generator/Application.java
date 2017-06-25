package ro.tucn.generator;

import org.apache.log4j.Logger;

/**
 * Created by Liviu on 4/5/2017.
 */
public class Application {

    private static final Logger logger = Logger.getLogger("AbstractGenerator");

    public static void main(String args[]) {
        if (args.length > 0) {
            new DataGenerator().run(args);
        }
    }
}
