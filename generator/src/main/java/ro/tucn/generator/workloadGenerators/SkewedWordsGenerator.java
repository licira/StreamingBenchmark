package ro.tucn.generator.workloadGenerators;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.Logger;
import ro.tucn.generator.entity.Sentence;
import ro.tucn.generator.helper.SentenceHelper;
import ro.tucn.generator.helper.TimeHelper;
import ro.tucn.generator.sender.AbstractMessageSender;
import ro.tucn.generator.sender.SentenceSender;
import ro.tucn.skewedWords.FastZipfGenerator;

import static ro.tucn.util.Topics.SKEWED_WORDS;

/**
 * Created by Liviu on 4/8/2017.
 */
public class SkewedWordsGenerator extends AbstractGenerator {

    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    private SentenceHelper sentenceHelper;
    private AbstractMessageSender sentenceSender;

    private long SENTENCE_NUM = 10;

    public SkewedWordsGenerator() {
        super();
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
        Sentence sentence = sentenceHelper.getNewSkewedWordsSentence();
        sentenceSender.send(sentence);
    }

    private void initializeHelper() {
        sentenceHelper = new SentenceHelper();
    }

    private void initializeMessageSenderWithSmallBuffer() {
        sentenceSender = new SentenceSender();
        sentenceSender.setTopic(SKEWED_WORDS);
        sentenceSender.initializeSmallBufferProducer(bootstrapServers);
    }

    private void shutdownSender() {
        sentenceSender.close();
    }

    @Override
    protected void submitData(int sleepFrequency) {
        for (long i = 0; i < SENTENCE_NUM; ++i) {
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
        int zipfSize = Integer.parseInt(this.properties.getProperty("zipf.size"));
        double zipfExponent = Double.parseDouble(this.properties.getProperty("zipf.exponent"));
        int wordsNumberLowerBound = Integer.parseInt(this.properties.getProperty("words.number.lower.bound"));
        int wordsNumberUpperBound = Integer.parseInt(this.properties.getProperty("words.number.upper.bound"));
        int wordIdLowerBound = Integer.parseInt(this.properties.getProperty("word.id.lower.bound"));
        RandomDataGenerator messageGenerator = new RandomDataGenerator();
        FastZipfGenerator zipfGenerator = new FastZipfGenerator(zipfSize, zipfExponent);
        sentenceHelper.setMessageGenerator(messageGenerator);
        sentenceHelper.setZipfGenerator(zipfGenerator);
        sentenceHelper.setWordsNumberLowerBound(wordsNumberLowerBound);
        sentenceHelper.setWordsNumberUpperBound(wordsNumberUpperBound);
        sentenceHelper.setWordIdLowerBound(wordIdLowerBound);
    }

    @Override
    protected void initializeWorkloadData() {
        double mu = Double.parseDouble(this.properties.getProperty("sentence.mu"));
        double sigma = Double.parseDouble(this.properties.getProperty("sentence.sigma"));
        sentenceHelper.setMu(mu);
        sentenceHelper.setSigma(sigma);
    }
}