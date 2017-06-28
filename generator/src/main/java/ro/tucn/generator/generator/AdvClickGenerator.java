package ro.tucn.generator.generator;

import org.apache.commons.math3.random.RandomDataGenerator;
import ro.tucn.DataMode;
import ro.tucn.generator.creator.entity.AdvCreator;
import ro.tucn.generator.creator.entity.ClickCreator;
import ro.tucn.generator.entity.Adv;
import ro.tucn.generator.entity.Click;
import ro.tucn.generator.helper.TimeHelper;
import ro.tucn.generator.sender.AbstractSender;
import ro.tucn.generator.sender.kafka.AbstractKafkaSender;
import ro.tucn.generator.sender.kafka.AdvSenderKafka;
import ro.tucn.generator.sender.kafka.ClickSenderKafka;
import ro.tucn.generator.sender.offline.AdvSenderOffline;
import ro.tucn.generator.sender.offline.ClickSenderOffline;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static ro.tucn.topic.KafkaTopics.ADV;
import static ro.tucn.topic.KafkaTopics.CLICK;

/**
 * Created by Liviu on 4/4/2017.
 */
public class AdvClickGenerator extends AbstractGenerator {

    private static Long totalAdvs;
    private AbstractSender advSender;
    private AbstractSender clickSender;
    private RandomDataGenerator generator;
    private ExecutorService cachedPool;
    private ArrayList<Adv> advs;
    private double clickLambda;
    private double clickProbability;
    private ClickCreator clickCreator;

    public AdvClickGenerator(String dataMode, int entitiesNumber) {
        super(entitiesNumber);
        initialize(dataMode);
    }

    @Override
    public void generate(int sleepFrequency) {
        initializePerformanceLogWithCurrentTime();
        performanceLog.disablePrint();
        submitData(sleepFrequency);
        performanceLog.logTotalThroughputAndTotalLatency();
        shutdownExecutorService();
        shutdownSender();
    }

    private void addToAdvList(Adv adv) {
        // long deltaT = (long) generator.nextExponential(clickLambda) * 1000;
        long deltaT = (long) generator.nextGaussian(clickLambda, 1) * totalAdvs;
        adv.setTimestamp(adv.getTimestamp() + deltaT);
        advs.add(adv);
    }

    private void initializeExecutorService() {
        // Obtain a cached thread pool
        cachedPool = Executors.newCachedThreadPool();
        // sub thread use variable in main thread
        // for loop to generate advertisement
    }

    private void initializeKafkaMessageSendersWithSmallBuffer() {
        advSender = new AdvSenderKafka();
        advSender.setTopic(ADV);
        ((AbstractKafkaSender)advSender).initializeSmallBufferProducer(bootstrapServers);
        clickSender = new ClickSenderKafka();
        clickSender.setTopic(CLICK);
        ((AbstractKafkaSender)clickSender).initializeSmallBufferProducer(bootstrapServers);
    }

    private void initializeOfflineMessageSenders() {
        advSender = new AdvSenderOffline();
        advSender.setTopic(ADV);
        clickSender = new ClickSenderOffline();
        clickSender.setTopic(CLICK);
    }

    private void shutdownExecutorService() {
        cachedPool.shutdown();
        try {
            cachedPool.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.info(e.getMessage());
        }
    }

    private void shutdownSender() {
        advSender.close();
        clickSender.close();
    }

    private Adv submitNewAdv() {
        Adv adv = AdvCreator.getNewAdv();
        advSender.send(adv);
        return adv;
    }

    private void submitNewClick() {
        for (Adv adv : advs) {
            // probability that the customer would click this advertisement
            if (generator.nextUniform(0, 1) <= clickProbability) {
                Click click = clickCreator.getNewClick(adv);
                clickSender.send(click);
                attemptSleep(adv.getTimestamp());
            }
        }
        advs.clear();
    }

    private void attemptSleep(long time) {
        long currentTime = System.nanoTime();
        if (currentTime < time) {
            try {
                Thread.sleep(time - currentTime);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

    private boolean clickSubmissionCondition(long value) {
        long submissionThreshold = (long) (clickProbability * totalAdvs);
        return (value % submissionThreshold == submissionThreshold - 1);
    }

    @Override
    protected void submitData(int sleepFrequency) {
        advs = new ArrayList();
        for (long i = 0; i < totalAdvs; ++i) {
            Adv adv = submitNewAdv();
            addToAdvList(adv);
            if (clickSubmissionCondition(i)) {
                submitNewClick();
            }
            performanceLog.logThroughputAndLatency(TimeHelper.getNanoTime());
            TimeHelper.temporizeDataGeneration(sleepFrequency, i);
        }
    }

    @Override
    protected void initialize(String dataMode) {
        initializeMessageSenders(dataMode);
        initializeWorkloadData();
        initializeDataGenerators();
        initializeExecutorService();
    }

    private void initializeMessageSenders(String dataMode) {
        if (dataMode.equalsIgnoreCase(DataMode.STREAMING)) {
            initializeKafkaMessageSendersWithSmallBuffer();
        } else if (dataMode.equalsIgnoreCase(DataMode.BATCH)) {
            initializeOfflineMessageSenders();
        }
    }

    @Override
    protected void initializeDataGenerators() {
        generator = new RandomDataGenerator();
        generator.reSeed(10000L);
        clickCreator = new ClickCreator();
    }

    @Override
    protected void initializeWorkloadData() {
        clickProbability = Double.parseDouble(properties.getProperty("click.probability"));
        clickLambda = Double.parseDouble(properties.getProperty("click.lambda"));
        totalAdvs = ((entitiesNumber == 0) ? Long.parseLong(properties.getProperty("adv.number")) : entitiesNumber);
    }
}
