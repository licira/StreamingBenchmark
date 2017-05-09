package ro.tucn.generator.workloadGenerators;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.Logger;
import ro.tucn.generator.helper.TimeHelper;
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

    @Override
    protected void generateData(int sleepFrequency) {
        for (long sentSentences = 0; sentSentences < SENTENCE_NUM; ++sentSentences) {
            StringBuilder messageData = buildMessageData();
            send(TOPIC, null, messageData.toString());
            performanceLog.logThroughputAndLatency(TimeHelper.getNanoTime());
            TimeHelper.temporizeDataGeneration(sleepFrequency, sentSentences);
        }
    }

    protected StringBuilder buildMessageData() {
        double sentenceLength = messageGenerator.nextGaussian(mu, sigma);
        StringBuilder messageData = new StringBuilder();
        for (int l = 0; l < sentenceLength; ++l) {
            int value = messageGenerator.nextInt(1, uniformSize);
            messageData.append(Utils.intToString(value));
            messageData.append(" ");
        }
        long timestamp = TimeHelper.getNanoTime();
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
        TOPIC = Topics.UNIFORM_WORDS;
    }

    @Override
    protected void initializeDataGenerators() {
        messageGenerator = new RandomDataGenerator();
    }

    @Override
    protected void initializeMessageSender() {

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