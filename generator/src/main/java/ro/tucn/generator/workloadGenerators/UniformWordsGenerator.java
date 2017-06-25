package ro.tucn.generator.workloadGenerators;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.Logger;
import ro.tucn.generator.entity.Sentence;
import ro.tucn.generator.helper.SentenceHelper;
import ro.tucn.generator.helper.TimeHelper;
import ro.tucn.generator.sender.AbstractMessageSender;
import ro.tucn.generator.sender.SentenceSender;

import static ro.tucn.util.Topics.UNIFORM_WORDS;

/**
 * Created by Liviu on 4/8/2017.
 */
public class UniformWordsGenerator extends AbstractGenerator {

    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    private SentenceHelper sentenceHelper;
    private AbstractMessageSender sentenceSender;

    private long totalSentences = 10;

    public UniformWordsGenerator(int entitiesNumber) {
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
        Sentence sentence = sentenceHelper.getNewUniformWordsSentence();
        sentenceSender.send(sentence);
    }

    private void shutdownSender() {
        sentenceSender.close();
    }

    private void initializeHelper() {
        sentenceHelper = new SentenceHelper();
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
        sentenceHelper.setMessageGenerator(messageGenerator);
    }

    @Override
    protected void initializeWorkloadData() {
        totalSentences = ((entitiesNumber == 0) ? Integer.parseInt(this.properties.getProperty("sentences.number")) : entitiesNumber);
        int upperBound = Integer.parseInt(this.properties.getProperty("uniform.size"));
        double mu = Double.parseDouble(this.properties.getProperty("sentence.mu"));
        double sigma = Double.parseDouble(this.properties.getProperty("sentence.sigma"));
        sentenceHelper.setMu(mu);
        sentenceHelper.setSigma(sigma);
        sentenceHelper.setUpperBound(upperBound);
    }
}