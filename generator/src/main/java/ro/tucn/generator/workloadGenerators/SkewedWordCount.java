package ro.tucn.generator.workloadGenerators;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import ro.tucn.skewedWords.FastZipfGenerator;
import ro.tucn.statistics.ThroughputLog;
import ro.tucn.util.Constants;
import ro.tucn.util.Topics;
import ro.tucn.util.Utils;

/**
 * Generator for WordCount workload
 * The distribution of works is skewed
 * Created by yangjun.wang on 26/10/15.
 */
public class SkewedWordCount extends Generator {

    private static final Logger logger = Logger.getLogger(SkewedWordCount.class);

    private static KafkaProducer<String, String> producer;
    private long SENTENCE_NUM = 1000;
    private int zipfSize;
    private double zipfExponent;
    private double mu;
    private double sigma;

    public SkewedWordCount() {
        super();
        producer = createLargeBufferProducer();
        initializeWorkloadData();
        TOPIC = Topics.SKEWED_WORDS;
    }

    /*
    public static void main(String[] args) throws InterruptedException {
        int SLEEP_FREQUENCY = -1;
        if (args.length > 0) {
            SLEEP_FREQUENCY = Integer.parseInt(args[0]);
        }
        new SkewedWordCount().generate(SLEEP_FREQUENCY);
    }
    */
    public void generate(int sleep_frequency) throws InterruptedException {
        RandomDataGenerator messageGenerator = new RandomDataGenerator();
        long time = System.currentTimeMillis();

        FastZipfGenerator zipfGenerator = new FastZipfGenerator(zipfSize, zipfExponent);
        ThroughputLog throughput = new ThroughputLog(this.getClass().getSimpleName());
        // for loop to generate message
        for (long sent_sentences = 0; sent_sentences < SENTENCE_NUM; ++sent_sentences) {
            double sentence_length = messageGenerator.nextGaussian(mu, sigma);
            StringBuilder messageBuilder = new StringBuilder();
            for (int l = 0; l < sentence_length; ++l) {
                int number = zipfGenerator.next();
                messageBuilder.append(Utils.intToString(number)).append(" ");
            }

            // Add timestamp
            messageBuilder.append(Constants.TimeSeparator).append(System.currentTimeMillis());

            throughput.execute();
            ProducerRecord<String, String> newRecord = new ProducerRecord<String, String>(TOPIC, messageBuilder.toString());
            producer.send(newRecord);

            // control data generate speed
            if (sleep_frequency > 0 && sent_sentences % sleep_frequency == 0) {
                //Thread.sleep(1);
            }
        }
        producer.close();
        logger.info("LatencyLog: " + String.valueOf(System.currentTimeMillis() - time));
    }

    protected void initializeWorkloadData() {
        zipfSize = Integer.parseInt(this.properties.getProperty("zipf.size", "1000"));
        zipfExponent = Double.parseDouble(this.properties.getProperty("zipf.exponent", "1"));
        mu = Double.parseDouble(this.properties.getProperty("sentence.mu", "10"));
        sigma = Double.parseDouble(this.properties.getProperty("sentence.sigma", "1"));
    }
}