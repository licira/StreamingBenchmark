package ro.tucn.generator;

import org.apache.log4j.Logger;
import ro.tucn.generator.creator.GeneratorCreator;
import ro.tucn.generator.generator.AbstractGenerator;
import ro.tucn.util.ArgsParser;

import java.util.HashMap;

/**
 * Created by Liviu on 4/5/2017.
 */
public class Application {

    private static final Logger logger = Logger.getLogger(Application.class);

    public static void main(String args[]) {
        if (args.length > 0) {
            HashMap<String, String> parsedArgs = ArgsParser.parseArgs(args);
            String topic = ArgsParser.getTopic(parsedArgs);
            int sleepFrequency = ArgsParser.getSleepFrequency(parsedArgs);
            int entitiesNumber = ArgsParser.getNumberOfGeneratedEntities(parsedArgs);
            logger.info(topic);
            AbstractGenerator generator = GeneratorCreator.getNewGenerator(topic, entitiesNumber);
            generator.generate(sleepFrequency);
        }
    }
}
