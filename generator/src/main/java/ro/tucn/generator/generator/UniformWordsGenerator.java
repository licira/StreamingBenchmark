package ro.tucn.generator.generator;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.Logger;
import ro.tucn.DataMode;
import ro.tucn.generator.creator.entity.SentenceCreator;
import ro.tucn.generator.entity.Sentence;
import ro.tucn.generator.helper.TimeHelper;
import ro.tucn.generator.sender.AbstractSender;
import ro.tucn.generator.sender.kafka.AbstractKafkaSender;
import ro.tucn.generator.sender.kafka.SentenceSenderKafka;
import ro.tucn.generator.sender.offline.AbstractOfflineSender;
import ro.tucn.generator.sender.offline.SentenceSenderOffline;

import java.util.List;
import java.util.Map;

import static ro.tucn.topic.KafkaTopics.UNIFORM_WORDS;

/**
 * Created by Liviu on 4/8/2017.
 */
public class UniformWordsGenerator extends AbstractGenerator {

    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    private SentenceCreator sentenceCreator;
    private AbstractSender sentenceSender;

    private long totalSentences = 10;

    public UniformWordsGenerator(String dataMode, int entitiesNumber) {
        super(entitiesNumber);
        initialize(dataMode);
    }

    @Override
    public void generate(int sleepFrequency) {
        initializePerformanceLogWithCurrentTime();
        performanceLog.disablePrint();
        submitData(sleepFrequency);
        performanceLog.logTotalThroughputAndTotalLatency();
        shutdownSender();
    }

    private void submitNewSentence() {
        Sentence sentence = sentenceCreator.getNewUniformWordsSentence();
        sentenceSender.send(sentence);
    }

    private void shutdownSender() {
        sentenceSender.close();
    }

    private void initializeHelper() {
        sentenceCreator = new SentenceCreator();
    }

    private void initializeKafkaMessageSenderWithSmallBuffer() {
        sentenceSender = new SentenceSenderKafka();
        sentenceSender.setTopic(UNIFORM_WORDS);
        ((AbstractKafkaSender)sentenceSender).initializeSmallBufferProducer(bootstrapServers);
        sentenceSender.setPerformanceLog(performanceLog);
    }

    private void initializeOfflineMessageSender() {
        sentenceSender = new SentenceSenderOffline();
        sentenceSender.setTopic(UNIFORM_WORDS);
        sentenceSender.setPerformanceLog(performanceLog);
    }

    @Override
    protected void submitData(int sleepFrequency) {
        for (long i = 0; i < totalSentences; ++i) {
            submitNewSentence();
            performanceLog.logThroughputAndLatency(TimeHelper.getNanoTime());
            TimeHelper.temporizeDataGeneration(sleepFrequency, i);
        }
    }

    @Override
    protected void initialize(String dataMode) {
        initializeHelper();
        initialiseMessageSender(dataMode);
        initializeWorkloadData();
        initializeDataGenerators();
    }

    private void initialiseMessageSender(String dataMode) {
        if (dataMode.equalsIgnoreCase(DataMode.STREAMING)) {
            initializeKafkaMessageSenderWithSmallBuffer();
        } else if (dataMode.equalsIgnoreCase(DataMode.BATCH)) {
            initializeOfflineMessageSender();
        }
    }

    @Override
    protected void initializeDataGenerators() {
        RandomDataGenerator messageGenerator = new RandomDataGenerator();
        sentenceCreator.setMessageGenerator(messageGenerator);
    }

    @Override
    protected void initializeWorkloadData() {
        totalSentences = ((entitiesNumber == 0) ? Integer.parseInt(this.properties.getProperty("sentences.number")) : entitiesNumber);
        int upperBound = Integer.parseInt(this.properties.getProperty("uniform.size"));
        double mu = Double.parseDouble(this.properties.getProperty("sentence.mu"));
        double sigma = Double.parseDouble(this.properties.getProperty("sentence.sigma"));
        sentenceCreator.setMu(mu);
        sentenceCreator.setSigma(sigma);
        sentenceCreator.setUpperBound(upperBound);
    }

    @Override
    public List<Map<String, String>> getGeneratedData(String topic) {
        return ((AbstractOfflineSender) sentenceSender).getGeneratedData(topic);
    }
}