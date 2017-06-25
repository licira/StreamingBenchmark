package ro.tucn.flink.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.Logger;
import ro.tucn.flink.kafka.FlinkKafkaConsumerCustom;
import ro.tucn.kafka.KafkaConsumerCustom;
import ro.tucn.operator.OperatorCreator;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Liviu on 4/17/2017.
 */
public class FlinkOperatorCreator extends OperatorCreator {

    private static final Logger logger = Logger.getLogger(FlinkOperatorCreator.class);

    private final StreamExecutionEnvironment env;
    private Properties properties;

    public FlinkOperatorCreator(String name) throws IOException {
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
    public KafkaConsumerCustom getKafkaConsumerCustom() {
        return new FlinkKafkaConsumerCustom(env);
    }

    private void initializeProperties() throws IOException {
        properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream("flink-cluster.properties"));
    }
}
