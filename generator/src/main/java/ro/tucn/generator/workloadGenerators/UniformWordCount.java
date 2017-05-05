package ro.tucn.generator.workloadGenerators;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.Logger;
import ro.tucn.util.Constants;
import ro.tucn.util.Topics;
import ro.tucn.util.Utils;

/**
 * Created by Liviu on 4/8/2017.
 */
public class UniformWordCount extends Generator {

    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    private RandomDataGenerator messageGenerator;

    private long SENTENCE_NUM = 10;
    private int uniformSize;
    private double mu;
    private double sigma;

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

    private void generateData(int sleepFrequency) {
        // for loop to generate message
        for (long sentSentences = 0; sentSentences < SENTENCE_NUM; ++sentSentences) {
            double sentenceLength = messageGenerator.nextGaussian(mu, sigma);
            StringBuilder messageBuilder = new StringBuilder();
            for (int l = 0; l < sentenceLength; ++l) {
                int number = messageGenerator.nextInt(1, uniformSize);
                messageBuilder.append(Utils.intToString(number)).append(" ");
            }

            long timestamp = System.nanoTime();
            messageBuilder.append(Constants.TimeSeparator).append(timestamp);

            send(TOPIC, null, messageBuilder.toString());

            performanceLog.logThroughputAndLatency(System.nanoTime());
            // control data generate speed
            if (sleepFrequency > 0 && sentSentences % sleepFrequency == 0) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
        }
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
        TOPIC = Topics.UNIFORM_WORDS;
    }

    @Override
    protected void initializeDataGenerators() {
        messageGenerator = new RandomDataGenerator();
    }

    @Override
    protected void initializeWorkloadData() {
        uniformSize = Integer.parseInt(this.properties.getProperty("uniform.size"));
        mu = Double.parseDouble(this.properties.getProperty("sentence.mu"));
        sigma = Double.parseDouble(this.properties.getProperty("sentence.sigma"));
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
}