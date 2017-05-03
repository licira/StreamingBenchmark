package ro.tucn.kafka_starter;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.IOException;
import java.util.Properties;

import static org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

/**
 * Created by Liviu on 5/3/2017.
 */
public class Zookeeper {

    private final Logger logger = Logger.getLogger(this.getClass());

    private ZooKeeperServerMain zooKeeperServerMain;
    private Properties properties;
    private ServerConfig serverConfig;
    private Runnable task;

    public Zookeeper() {
        try {
            initialize();
        } catch (IOException | ConfigException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

    public void start() {
        new Thread(task).start();
    }

    private void initialize() throws IOException, ConfigException {
        initializeProperties();
        initializeZookeeperConfig();
        initializeZookeeper();
        initializeTask();
    }

    private void initializeProperties() throws IOException {
        properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream("zookeeper.properties"));
    }

    private void initializeZookeeperConfig() throws IOException, ConfigException {
        QuorumPeerConfig quorumConfiguration = getQuorumConfigurationFromProperties(properties);
        serverConfig = new ServerConfig();
        serverConfig.readFrom(quorumConfiguration);
    }

    private void initializeZookeeper() {
        zooKeeperServerMain = new ZooKeeperServerMain();
    }

    private void initializeTask() {
        task = () -> {
            try {
                zooKeeperServerMain.runFromConfig(serverConfig);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        };
    }

    private QuorumPeerConfig getQuorumConfigurationFromProperties(Properties properties) throws IOException, ConfigException {
        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
        quorumConfiguration.parseProperties(properties);
        return quorumConfiguration;
    }

    public Runnable getTask() {
        return task;
    }
}