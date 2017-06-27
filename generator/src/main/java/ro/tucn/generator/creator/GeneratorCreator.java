package ro.tucn.generator.creator;

import ro.tucn.generator.generator.*;
import ro.tucn.topic.ApplicationTopics;

/**
 * Created by Liviu on 6/27/2017.
 */
public class GeneratorCreator {

    private static final String NONEXISTING_GENERATOR_EXCEPTION_MSG ="No generator available for this.";

    public static AbstractGenerator getNewGenerator(String topic, int entitiesNumber) {
        AbstractGenerator generator;
        if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.ADV))) {
            generator = new AdvClickGenerator(entitiesNumber);
        } else if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.K_MEANS))) {
            generator = new KMeansGenerator(entitiesNumber);
        } else if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.SKEWED_WORDS))) {
            generator = new SkewedWordsGenerator(entitiesNumber);
        } else if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.UNIFORM_WORDS))) {
            generator = new UniformWordsGenerator(entitiesNumber);
        } else {
            throw new RuntimeException(NONEXISTING_GENERATOR_EXCEPTION_MSG);
        }
        return generator;
    }
}
