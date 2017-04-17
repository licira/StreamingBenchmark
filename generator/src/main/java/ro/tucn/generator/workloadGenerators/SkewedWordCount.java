package ro.tucn.generator.workloadGenerators;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import ro.tucn.skewedWords.FastZipfGenerator;
import ro.tucn.util.Constants;
import ro.tucn.util.Topics;
import ro.tucn.util.Utils;

/**
 * Created by Liviu on 4/8/2017.
 */
public class SkewedWordCount extends Generator {

    private static final Logger logger = Logger.getLogger(SkewedWordCount.class.getSimpleName());

    private static KafkaProducer<String, String> producer;
    private long SENTENCE_NUM = 1000;
    private int zipfSize;
    private double zipfExponent;
    private double mu;
    private double sigma;

    public SkewedWordCount() {
        super();
        producer = createLargeBufferProducer();
        TOPIC = Topics.SKEWED_WORDS;
        initializeWorkloadData();
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
    public void generate(int sleepFrequency) {
        RandomDataGenerator messageGenerator = new RandomDataGenerator();
        long time = System.nanoTime();

        FastZipfGenerator zipfGenerator = new FastZipfGenerator(zipfSize, zipfExponent);

        initializePerformanceLogWithCurrentTime();
        // for loop to generate message
        for (long sentSentences = 0; sentSentences < SENTENCE_NUM; ++sentSentences) {
            double sentenceLength = messageGenerator.nextGaussian(mu, sigma);
            StringBuilder messageBuilder = new StringBuilder();
            for (int l = 0; l < sentenceLength; ++l) {
                int number = zipfGenerator.next();
                messageBuilder.append(Utils.intToString(number)).append(" ");
            }

            // Add timestamp
            messageBuilder.append(Constants.TimeSeparator).append(System.nanoTime());

            ProducerRecord<String, String> newRecord = new ProducerRecord<String, String>(TOPIC, messageBuilder.toString());
            producer.send(newRecord);
            performanceLog.logThroughputAndLatency(System.nanoTime());
            // control data generate speed
            if (sleepFrequency > 0 && sentSentences % sleepFrequency == 0) {
                //Thread.sleep(1);
            }
        }
        performanceLog.logTotalThroughputAndTotalLatency();
        producer.close();
    }

    protected void initializeWorkloadData() {
        zipfSize = Integer.parseInt(this.properties.getProperty("zipf.size", "1000"));
        zipfExponent = Double.parseDouble(this.properties.getProperty("zipf.exponent", "1"));
        mu = Double.parseDouble(this.properties.getProperty("sentence.mu", "10"));
        sigma = Double.parseDouble(this.properties.getProperty("sentence.sigma", "1"));
    }
}