package ro.tucn.storm.operator;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.OperatorCreator;
import ro.tucn.operator.Operator;
import ro.tucn.operator.PairOperator;
import ro.tucn.storm.bolt.BoltWithTime;
import ro.tucn.util.ConfigReader;
import ro.tucn.util.WithTime;
import storm.kafka.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by Liviu on 4/17/2017.
 */
public class StormOperatorCreator extends OperatorCreator implements Serializable {

    private Properties properties;
    private Config conf;
    private TopologyBuilder topologyBuilder;

    public StormOperatorCreator(String name) throws IOException {
        super(name);
        initializeProperties();
        initializeConfig();
        initializeTopologyBuilder();
    }

    private void initializeTopologyBuilder() {
        topologyBuilder = new TopologyBuilder();
    }

    private void initializeConfig() {
        conf = new Config();
        //conf.setDebug(true);
        //ack enabled
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 100);
        // ack disable
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60);
    }

    private void initializeProperties() {
        try {
            properties = ConfigReader.getPropertiesFromResourcesFile("storm-cluster.properties");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Operator<String> getStringStreamFromKafka(Properties properties, String topicPropertyName, String componentId, int parallelism) {
        conf.setNumWorkers(parallelism);
        SpoutConfig spoutConfig = createSpoutConfig(properties);
        topologyBuilder.setSpout(componentId, new KafkaSpout(spoutConfig), parallelism);
        return new StormOperator<>(topologyBuilder, componentId, parallelism);
    }

    @Override
    public PairOperator<String, String> getPairStreamFromKafka(Properties properties, String topicPropertyName, String componentId, int parallelism) {
        return null;
    }

    @Override
    public Operator<WithTime<String>> getStringStreamWithTimeFromKafka(Properties properties, String topicPropertyName, String componentId, int parallelism) {
        conf.setNumWorkers(parallelism);
        SpoutConfig spoutConfig = createSpoutConfig(properties);
        topologyBuilder.setSpout("spout", new KafkaSpout(spoutConfig), parallelism);
        topologyBuilder.setBolt("addTime", new BoltWithTime<String>(), parallelism).localOrShuffleGrouping("spout");
        return new StormOperator<>(topologyBuilder, "addTime", parallelism);
    }

    @Override
    public Operator<Point> getPointStreamFromKafka(Properties properties, String topicPropertyName, String componentId, int parallelism) {
        conf.setNumWorkers(parallelism);
        SpoutConfig spoutConfig = createSpoutConfig(properties);
        topologyBuilder.setSpout(componentId, new KafkaSpout(spoutConfig), parallelism);
        return new StormOperator<>(topologyBuilder, componentId, parallelism);
    }

    /*
        @Override
        public Operator<WithTime<String>> getStringStreamWithTimeFromKafka(String zkConStr,
                                                                                String kafkaServers,
                                                                                String group,
                                                                                String topics,
                                                                                String offset,
                                                                                String componentId,
                                                                                int parallelism) {
            conf.setNumWorkers(parallelism);
            BrokerHosts hosts = new ZkHosts(zkConStr);
            SpoutConfig spoutConfig = new SpoutConfig(hosts, topics, "/" + topics, UUID.randomUUID().toString());
            spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
            if (offset.endsWith("smallest")) {
                spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
            }
            spoutConfig.fetchSizeBytes = 1024;
            spoutConfig.bufferSizeBytes = 1024;
    //        spoutConfig.ignoreZkOffsets = true;

            topologyBuilder.setSpout("spout", new KafkaSpout(spoutConfig), parallelism);
            topologyBuilder.setBolt("addTime", new BoltWithTime<String>(), parallelism).localOrShuffleGrouping("spout");
            return new StormOperator<>(topologyBuilder, "addTime", parallelism);
        }

        @Override
        public Operator<Point> getPointStreamFromKafka(String zkConStr, String kafkaServers, String group, String topics, String offset, String componentId, int parallelism) {
            conf.setNumWorkers(parallelism);
            BrokerHosts hosts = new ZkHosts(zkConStr);
            SpoutConfig spoutConfig = new SpoutConfig(hosts, topics, "/" + topics, UUID.randomUUID().toString());
            spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
            if (offset.endsWith("smallest")) {
                spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
            }
            spoutConfig.fetchSizeBytes = 1024;
            spoutConfig.bufferSizeBytes = 1024;
    //        spoutConfig.ignoreZkOffsets = true;

            topologyBuilder.setSpout("spout", new KafkaSpout(spoutConfig), parallelism);
            topologyBuilder.setBolt("extractPoint", new ExtractPointBolt(), parallelism).localOrShuffleGrouping("spout");
            return new StormOperator<>(topologyBuilder, "extractPoint", parallelism);
        }

        @Override
        public Operator<String> getStringStreamFromKafka(String zkConStr,
                                                              String kafkaServers,
                                                              String group,
                                                              String topics,
                                                              String offset,
                                                              String componentId,
                                                              int parallelism) {
            conf.setNumWorkers(parallelism);
            BrokerHosts hosts = new ZkHosts(zkConStr);
            SpoutConfig spoutConfig = new SpoutConfig(hosts, topics, "/" + topics, UUID.randomUUID().toString());
            spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
            spoutConfig.fetchSizeBytes = 1024;
            spoutConfig.bufferSizeBytes = 1024;
    //        spoutConfig.ignoreZkOffsets = true;

            topologyBuilder.setSpout(componentId, new KafkaSpout(spoutConfig), parallelism);
            return new StormOperator<>(topologyBuilder, componentId, parallelism);
        }
        */
    @Override
    public void Start() {
        // TODO: switch between local and cluster
        try {
            StormSubmitter.submitTopologyWithProgressBar(appName, conf, topologyBuilder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }
        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("word-count", conf, topologyBuilder.createTopology());
    }

    private SpoutConfig createSpoutConfig(Properties properties) {
        BrokerHosts hosts = new ZkHosts((String) properties.get("zookeeper.connect"));
        String topics = (String) properties.get("topic1");
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topics, "/" + topics, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        String offset = (String) properties.get("auto.offset.reset");
        if (offset.endsWith("smallest")) {
            spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        }
        spoutConfig.fetchSizeBytes = 1024;
        spoutConfig.bufferSizeBytes = 1024;
        //spoutConfig.ignoreZkOffsets = true;
        return spoutConfig;
    }
}