package ro.tucn.generator.workloadGenerators;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.Logger;
import ro.tucn.generator.entity.Sentence;
import ro.tucn.generator.helper.SentenceHelper;
import ro.tucn.generator.helper.TimeHelper;
import ro.tucn.generator.sender.AbstractMessageSender;
import ro.tucn.generator.sender.SentenceSender;
import ro.tucn.util.Topics;
import ro.tucn.util.Utils;

/**
 * Created by Liviu on 4/8/2017.
 */
public class UniformWordCount extends Generator {

    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    private SentenceHelper sentenceHelper;
    private AbstractMessageSender sentenceSender;

    private long SENTENCE_NUM = 10;

    public UniformWordCount() {
        super();
        initialize();
    }

    @Override
    public void generate(int sleepFrequency) {
        initializePerformanceLogWithCurrentTime();
        performanceLog.disablePrint();
        generateData(sleepFrequency);
        performanceLog.logTotalThroughputAndTotalLatency();
        producer.close();
    }


    private void submitNewSentence() {
        Sentence sentence = sentenceHelper.createNewUniformWordsSentence();
        sentenceSender.send(sentence);
    }

    private void initializeHelper() {
        sentenceHelper = new SentenceHelper();
    }

    @Override
    protected void generateData(int sleepFrequency) {
        for (long i = 0; i < SENTENCE_NUM; ++i) {
            submitNewSentence();
            performanceLog.logThroughputAndLatency(TimeHelper.getNanoTime());
            TimeHelper.temporizeDataGeneration(sleepFrequency, i);
        }
    }

    @Override
    protected void initialize() {
        initializeTopic();
        initializeHelper();
        initializeSmallBufferProducer();
        initializeWorkloadData();
        initializeDataGenerators();
    }

    @Override
    protected void initializeTopic() {
        TOPIC = Topics.UNIFORM_WORDS;
    }

    @Override
    protected void initializeDataGenerators() {
        RandomDataGenerator messageGenerator = new RandomDataGenerator();
        sentenceHelper.setMessageGenerator(messageGenerator);
    }

    @Override
    protected void initializeMessageSender() {
        sentenceSender = new SentenceSender();
    }

    @Override
    protected void initializeWorkloadData() {
        int upperBound = Integer.parseInt(this.properties.getProperty("uniform.size"));
        double mu = Double.parseDouble(this.properties.getProperty("sentence.mu"));
        double sigma = Double.parseDouble(this.properties.getProperty("sentence.sigma"));
        sentenceHelper.setMu(mu);
        sentenceHelper.setSigma(sigma);
        sentenceHelper.setUpperBound(upperBound);
    }
}