package ro.tucn.flink.context;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.Logger;
import ro.tucn.consumer.AbstractGeneratorConsumer;
import ro.tucn.flink.consumer.FlinkGeneratorConsumer;
import ro.tucn.flink.consumer.FlinkKafkaConsumerCustom;
import ro.tucn.consumer.AbstractKafkaConsumerCustom;
import ro.tucn.context.ContextCreator;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Liviu on 4/17/2017.
 */
public class FlinkContextCreator extends ContextCreator {

    private static final Logger logger = Logger.getLogger(FlinkContextCreator.class);

    private static final String FLINK_PROPERTIES_FILE_NAME = "flink-cluster.properties";

    private final StreamExecutionEnvironment env;
    private Properties properties;

    public FlinkContextCreator(String name) throws IOException {
        super(name);
        System.out.println("1");
        initializeProperties();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println("2");
    }

    @Override
    public void Start() {
        try {
            env.execute(appName);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public AbstractKafkaConsumerCustom getKafkaConsumerCustom() {
        return new FlinkKafkaConsumerCustom(env);
    }

    @Override
    public AbstractGeneratorConsumer getGeneratorConsumer() {
        return new FlinkGeneratorConsumer();
    }

    private void initializeProperties() throws IOException {
        properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream(FLINK_PROPERTIES_FILE_NAME));
    }
}
