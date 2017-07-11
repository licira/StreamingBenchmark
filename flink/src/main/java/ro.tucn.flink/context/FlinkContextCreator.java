package ro.tucn.flink.context;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.Logger;
import ro.tucn.DataMode;
import ro.tucn.consumer.AbstractGeneratorConsumer;
import ro.tucn.consumer.AbstractKafkaConsumerCustom;
import ro.tucn.context.ContextCreator;
import ro.tucn.flink.consumer.FlinkGeneratorConsumer;
import ro.tucn.flink.consumer.FlinkKafkaConsumerCustom;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Liviu on 4/17/2017.
 */
public class FlinkContextCreator extends ContextCreator {

    private static final Logger logger = Logger.getLogger(FlinkContextCreator.class);

    private static final String FLINK_PROPERTIES_FILE_NAME = "flink-cluster.properties";

    private StreamExecutionEnvironment streamEnv = null;
    private ExecutionEnvironment batchEnv = null;
    private Properties properties;
    private String dataMode;

    public FlinkContextCreator(String name, String dataMode, int parallelism) throws IOException {
        super(name);
        this.parallelism = parallelism;
        initializeProperties();
        initializeEnv(dataMode);
    }

    private void initializeEnv(String dataMode) {
        this.dataMode = dataMode;
        if (this.dataMode.equals(DataMode.STREAMING)) {
            streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            streamEnv.setParallelism(parallelism);
        } else if (this.dataMode.equals(DataMode.BATCH)) {
            batchEnv = ExecutionEnvironment.getExecutionEnvironment();
            batchEnv.setParallelism(parallelism);
        }
    }

    @Override
    public void start() {
        try {
            if (dataMode.equals(DataMode.STREAMING)) {
                streamEnv.execute(appName);
            } else if (dataMode.equals(DataMode.BATCH)) {
                batchEnv.execute(appName);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public AbstractKafkaConsumerCustom getKafkaConsumerCustom() {
        return new FlinkKafkaConsumerCustom(streamEnv);
    }

    @Override
    public AbstractGeneratorConsumer getGeneratorConsumer() {
        return new FlinkGeneratorConsumer(batchEnv);
    }

    private void initializeProperties() throws IOException {
        properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream(FLINK_PROPERTIES_FILE_NAME));
        if ((parallelism == 0) || (parallelism == -1)) {
            try {
                parallelism = Integer.parseInt(String.valueOf(properties.get("parallelism")));
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
            if (parallelism == -1) {
                parallelism = 1;
            }
        }
    }
}
