package ro.tucn.kafka_starter;

import org.apache.log4j.Logger;

/**
 * Created by Liviu on 5/3/2017.
 */
public class Application {

    private final Logger logger = Logger.getLogger(Application.class);
    private final int KAFKA_THREAD_POOL_SIZE = 2;
    private Zookeeper zookeeper;
    private Broker broker;

    public static void main(String[] args) {
        Application kafka = new Application();
        kafka.initialize();
        kafka.run();
    }

    public void initialize() {
        initializeZookeeper();
        initializeBroker();
    }

    private void run() {
        zookeeper.start();
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
        broker.start();
    }

    private void initializeZookeeper() {
        zookeeper = new Zookeeper();
    }

    private void initializeBroker() {
        broker = new Broker(zookeeper);
    }
}
