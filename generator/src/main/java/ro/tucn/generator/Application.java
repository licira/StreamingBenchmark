package ro.tucn.generator;

import org.apache.log4j.Logger;
import ro.tucn.generator.generator.*;
import ro.tucn.topic.ApplicationTopics;
import ro.tucn.util.ArgsParser;

import java.util.HashMap;

/**
 * Created by Liviu on 4/5/2017.
 */
public class Application {

    private static final Logger logger = Logger.getLogger("AbstractGenerator");

    public static void main(String args[]) {
        if (args.length > 0) {
            HashMap<String, String> parsedArgs = ArgsParser.parseArgs(args);
            String topic = ArgsParser.getTopic(parsedArgs);
            int sleepFrequency = ArgsParser.getSleepFrequency(parsedArgs);
            int entitiesNumber = ArgsParser.getNumberOfGeneratedEntities(parsedArgs);
            logger.info(topic);
            AbstractGenerator generator = null;
            if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.ADV))) {
                generator = new AdvClickGenerator(entitiesNumber);
            } else if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.K_MEANS))) {
                generator = new KMeansGenerator(entitiesNumber);
            } else if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.SKEWED_WORDS))) {
                generator = new SkewedWordsGenerator(entitiesNumber);
            } else if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.UNIFORM_WORDS))) {
                generator = new UniformWordsGenerator(entitiesNumber);
            } else {
                return;
            }
            generator.generate(sleepFrequency);
        }
    }
}
