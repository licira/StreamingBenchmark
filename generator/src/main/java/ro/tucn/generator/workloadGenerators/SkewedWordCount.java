package ro.tucn.generator.workloadGenerators;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.Logger;
import ro.tucn.skewedWords.FastZipfGenerator;
import ro.tucn.util.Topics;
import ro.tucn.util.Utils;

/**
 * Created by Liviu on 4/8/2017.
 */
public class SkewedWordCount extends Generator {

    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    private RandomDataGenerator messageGenerator;
    private FastZipfGenerator zipfGenerator;

    private long SENTENCE_NUM = 10;
    private int zipfSize;
    private double zipfExponent;
    private double mu;
    private double sigma;

    public SkewedWordCount() {
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

    @Override
    protected void generateData(int sleepFrequency) {
        for (long sentSentences = 0; sentSentences < SENTENCE_NUM; ++sentSentences) {
            StringBuilder messageData = buildMessageData();
            send(TOPIC, null, messageData.toString());
            performanceLog.logThroughputAndLatency(getNanoTime());
            temporizeDataGeneration(sleepFrequency, sentSentences);
        }
    }

    @Override
    protected StringBuilder buildMessageData() {
        double sentenceLength = messageGenerator.nextGaussian(mu, sigma);
        StringBuilder messageData = new StringBuilder();
        for (int l = 0; l < sentenceLength; ++l) {
            int value = zipfGenerator.next();
            messageData.append(Utils.intToString(value));
        }
        long timestamp = getNanoTime();
        //messageData.append(Constants.TimeSeparator).append(timestamp);
        return messageData;
    }

    @Override
    protected void initialize() {
        initializeTopic();
        initializeSmallBufferProducer();
        initializeWorkloadData();
        initializeDataGenerators();
    }

    @Override
    protected void initializeTopic() {
        TOPIC = Topics.SKEWED_WORDS;
    }

    @Override
    protected void initializeDataGenerators() {
        messageGenerator = new RandomDataGenerator();
        zipfGenerator = new FastZipfGenerator(zipfSize, zipfExponent);
    }

    @Override
    protected void initializeWorkloadData() {
        zipfSize = Integer.parseInt(this.properties.getProperty("zipf.size"));
        zipfExponent = Double.parseDouble(this.properties.getProperty("zipf.exponent"));
        mu = Double.parseDouble(this.properties.getProperty("sentence.mu"));
        sigma = Double.parseDouble(this.properties.getProperty("sentence.sigma"));
    }
    /*
    public static void main(String[] args) throws InterruptedException {
        int sleepFrequency = -1;
        if (args.length > 0) {
            sleepFrequency = Integer.parseInt(args[0]);
        }
        new SkewedWordCount().generate(sleepFrequency);
    }
    */
}