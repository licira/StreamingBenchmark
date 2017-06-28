package ro.tucn.generator.generator;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.Logger;
import ro.tucn.generator.entity.Sentence;
import ro.tucn.generator.creator.entity.SentenceCreator;
import ro.tucn.generator.helper.TimeHelper;
import ro.tucn.generator.sender.kafka.AbstractKafkaSender;
import ro.tucn.generator.sender.kafka.SentenceSender;

import static ro.tucn.topic.KafkaTopics.UNIFORM_WORDS;

/**
 * Created by Liviu on 4/8/2017.
 */
public class UniformWordsGenerator extends AbstractGenerator {

    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    private SentenceCreator SentenceCreator;
    private AbstractKafkaSender sentenceSender;

    private long totalSentences = 10;

    public UniformWordsGenerator(String dataMode, int entitiesNumber) {
        super(entitiesNumber);
        initialize();
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
        Sentence sentence = SentenceCreator.getNewUniformWordsSentence();
        sentenceSender.send(sentence);
    }

    private void shutdownSender() {
        sentenceSender.close();
    }

    private void initializeHelper() {
        SentenceCreator = new SentenceCreator();
    }

    private void initializeMessageSenderWithSmallBuffer() {
        sentenceSender = new SentenceSender();
        sentenceSender.setTopic(UNIFORM_WORDS);
        sentenceSender.initializeSmallBufferProducer(bootstrapServers);
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
    protected void initialize() {
        initializeHelper();
        initializeMessageSenderWithSmallBuffer();
        initializeWorkloadData();
        initializeDataGenerators();
    }

    @Override
    protected void initializeDataGenerators() {
        RandomDataGenerator messageGenerator = new RandomDataGenerator();
        SentenceCreator.setMessageGenerator(messageGenerator);
    }

    @Override
    protected void initializeWorkloadData() {
        totalSentences = ((entitiesNumber == 0) ? Integer.parseInt(this.properties.getProperty("sentences.number")) : entitiesNumber);
        int upperBound = Integer.parseInt(this.properties.getProperty("uniform.size"));
        double mu = Double.parseDouble(this.properties.getProperty("sentence.mu"));
        double sigma = Double.parseDouble(this.properties.getProperty("sentence.sigma"));
        SentenceCreator.setMu(mu);
        SentenceCreator.setSigma(sigma);
        SentenceCreator.setUpperBound(upperBound);
    }
}