package ro.tucn.generator.workloadGenerators;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.Logger;
import ro.tucn.skewedWords.FastZipfGenerator;
import ro.tucn.util.Constants;
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
        // for loop to generate message
        for (long sentSentences = 0; sentSentences < SENTENCE_NUM; ++sentSentences) {
            double sentenceLength = messageGenerator.nextGaussian(mu, sigma);
            StringBuilder messageBuilder = new StringBuilder();
            for (int l = 0; l < sentenceLength; ++l) {
                int number = zipfGenerator.next();
                messageBuilder.append(Utils.intToString(number)).append(" ");
            }

            long timestamp = getNanoTime();
            messageBuilder.append(Constants.TimeSeparator).append(timestamp);

            send(TOPIC, null, messageBuilder.toString());

            performanceLog.logThroughputAndLatency(getNanoTime());
            // control data generate speed
            if (sleepFrequency > 0 && sentSentences % sleepFrequency == 0) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        performanceLog.logTotalThroughputAndTotalLatency();
        producer.close();
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