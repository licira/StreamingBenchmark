package ro.tucn.generator;

import org.apache.log4j.Logger;
import ro.tucn.generator.workloadGenerators.*;
import ro.tucn.util.ArgsParser;
import ro.tucn.topic.KafkaTopics;

import java.util.HashMap;

/**
 * Created by Liviu on 4/5/2017.
 */
public class DataGenerator {

    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    public void run(String[] args) {
        HashMap<String, String> parsedArgs = ArgsParser.parseArgs(args);
        String topic = ArgsParser.getTopic(parsedArgs);
        int sleepFrequency = ArgsParser.getSleepFrequency(parsedArgs);
        int entitiesNumber = ArgsParser.getNumberOfGeneratedEntities(parsedArgs);
        logger.info(topic);
        AbstractGenerator generator = null;
        if (topic.equalsIgnoreCase(KafkaTopics.ADV)) {
            generator = new AdvClickGenerator(entitiesNumber);
        } else if (topic.equalsIgnoreCase(KafkaTopics.K_MEANS)) {
            generator = new KMeansGenerator(entitiesNumber);
        } else if (topic.equalsIgnoreCase(KafkaTopics.SKEWED_WORDS)) {
            generator = new SkewedWordsGenerator(entitiesNumber);
        } else if (topic.equalsIgnoreCase(KafkaTopics.UNIFORM_WORDS)) {
            generator = new UniformWordsGenerator(entitiesNumber);
        } else {
            return;
        }
        generator.generate(sleepFrequency);
    }
}
