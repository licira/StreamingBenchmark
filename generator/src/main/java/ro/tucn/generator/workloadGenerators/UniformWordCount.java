package ro.tucn.generator.workloadGenerators;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import ro.tucn.statistics.ThroughputLog;
import ro.tucn.util.Constants;
import ro.tucn.util.Topics;
import ro.tucn.util.Utils;

/**
 * Created by Liviu on 4/8/2017.
 */
public class UniformWordCount extends Generator {

    private static final Logger logger = Logger.getLogger(UniformWordCount.class);

    private static KafkaProducer<String, String> producer;
    private long SENTENCE_NUM = 1000;
    private int uniformSize;
    private double mu;
    private double sigma;

    public UniformWordCount() {
        super();
        producer = createLargeBufferProducer();
        initializeWorkloadData();
        TOPIC = Topics.UNIFORM_WORDS;
    }

    public void generate(int sleepFrequency) throws InterruptedException {

        RandomDataGenerator messageGenerator = new RandomDataGenerator();
        long time = System.currentTimeMillis();

        ThroughputLog throughput = new ThroughputLog(this.getClass().getSimpleName());
        // for loop to generate message
        for (long sentSentences = 0; sentSentences < SENTENCE_NUM; ++sentSentences) {
            double sentenceLength = messageGenerator.nextGaussian(mu, sigma);
            StringBuilder messageBuilder = new StringBuilder();
            for (int l = 0; l < sentenceLength; ++l) {
                int number = messageGenerator.nextInt(1, uniformSize);
                messageBuilder.append(Utils.intToString(number)).append(" ");
            }

            // Add timestamp
            messageBuilder.append(Constants.TimeSeparator).append(System.currentTimeMillis());
            throughput.execute();
            ProducerRecord<String, String> newRecord = new ProducerRecord<String, String>(TOPIC, messageBuilder.toString());
            producer.send(newRecord);

            // control data generate speed
            if (sleepFrequency > 0 && sentSentences % sleepFrequency == 0) {
                //Thread.sleep(1);
            }
        }
        producer.close();
        logger.info("LatencyLog: " + String.valueOf(System.currentTimeMillis() - time));
    }
    /*
    public static void main(String[] args) throws InterruptedException {
        int SLEEP_FREQUENCY = -1;
        if (args.length > 0) {
            SLEEP_FREQUENCY = Integer.parseInt(args[0]);
        }
        new UniformWordCount().generate(SLEEP_FREQUENCY);
    }
    */
    protected void initializeWorkloadData() {
        uniformSize = Integer.parseInt(this.properties.getProperty("uniform.size", "1000"));
        mu = Double.parseDouble(this.properties.getProperty("sentence.mu", "10"));
        sigma = Double.parseDouble(this.properties.getProperty("sentence.sigma", "1"));
    }
}