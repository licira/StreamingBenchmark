package ro.tucn.generator.creator;

import ro.tucn.DataMode;
import ro.tucn.generator.generator.*;
import ro.tucn.topic.ApplicationTopics;

/**
 * Created by Liviu on 6/27/2017.
 */
public class GeneratorCreator {

    private static final String NONEXISTING_GENERATOR_MSG ="No generator available for this.";

    public static AbstractGenerator getNewGenerator(String topic, String mode, int entitiesNumber) {
        AbstractGenerator generator;
        if (mode == null) {
            mode = DataMode.STREAMING;
        }
        if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.ADV))) {
            generator = new AdvClickGenerator(mode, entitiesNumber);
        } else if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.K_MEANS))) {
            generator = new KMeansGenerator(mode, entitiesNumber);
        } else if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.SKEWED_WORDS))) {
            generator = new SkewedWordsGenerator(mode, entitiesNumber);
        } else if (topic.equalsIgnoreCase(String.valueOf(ApplicationTopics.UNIFORM_WORDS))) {
            generator = new UniformWordsGenerator(mode, entitiesNumber);
        } else {
            throw new RuntimeException(NONEXISTING_GENERATOR_MSG);
        }
        return generator;
    }
}
