package ro.tucn.generator.workloadGenerators;

import org.apache.commons.math3.random.RandomDataGenerator;
import ro.tucn.generator.entity.Adv;
import ro.tucn.generator.entity.Click;
import ro.tucn.generator.helper.AdvHelper;
import ro.tucn.generator.helper.TimeHelper;
import ro.tucn.util.Topics;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Liviu on 4/4/2017.
 */
public class AdvClick extends Generator {

    private static String ADV_TOPIC = Topics.ADV;
    private static String CLICK_TOPIC = Topics.CLICK;
    private static Long advNum;
    private double clickLambda;
    private double clickProbability;

    private RandomDataGenerator generator;
    private ExecutorService cachedPool;
    private ArrayList<Adv> advList;

    public AdvClick() {
        super();
        initialize();
    }

    @Override
    public void generate(int sleepFrequency) {
        initializePerformanceLogWithCurrentTime();
        performanceLog.disablePrint();
        generateData(sleepFrequency);
        performanceLog.logTotalThroughputAndTotalLatency();
        shutdownExecutorService();
        producer.close();
    }

    private void addToAdvList(Adv adv) {
        // long deltaT = (long) generator.nextExponential(clickLambda) * 1000;
        long deltaT = (long) generator.nextGaussian(clickLambda, 1) * advNum;
        adv.setTime(adv.getTime() + deltaT);
        advList.add(adv);
    }

    private void initializeExecutorService() {
        // Obtain a cached thread pool
        cachedPool = Executors.newCachedThreadPool();
        // sub thread use variable in main thread
        // for loop to generate advertisement
    }

    private void shutdownExecutorService() {
        cachedPool.shutdown();
        try {
            cachedPool.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.info(e.getMessage());
        }
    }

    @Override
    protected void generateData(int sleepFrequency) {
        advList = new ArrayList();
        for (long i = 0; i < advNum; ++i) {
            Adv adv = submitNewAdv();
            addToAdvList(adv);
            if (clickSubmissionCondition(i)) {
                submitNewClick();
            }
            performanceLog.logThroughputAndLatency(TimeHelper.getNanoTime());
            TimeHelper.temporizeDataGeneration(sleepFrequency, i);
        }
    }

    private Adv submitNewAdv() {
        Adv adv = AdvHelper.createNewAdv();
        submitAdv(adv);
        return adv;
    }

    private void submitAdv(Adv adv) {
        StringBuilder messageValue = getAdvMessageDataValue(adv);
        StringBuilder messageKey = getAdvMessageDataKey(adv);
        send(ADV_TOPIC, messageKey.toString(), messageValue.toString());
    }

    private void submitNewClick() {
        for (Adv adv : advList) {
            // probability that the customer would click this advertisement
            if (generator.nextUniform(0, 1) <= clickProbability) {
                Click click = new Click(adv);
                StringBuilder messageValue = getClickMessageDataValue(click);
                StringBuilder messageKey = getClickMessageDataKey(click);
                //send(CLICK_TOPIC, null, messageData.toString());
                send(CLICK_TOPIC, messageKey.toString(), messageValue.toString());
                long currentTime = System.nanoTime();
                if (currentTime < adv.getTime()) {
                    try {
                        Thread.sleep(adv.getTime() - currentTime);
                    } catch (InterruptedException e) {
                        logger.error(e.getMessage());
                    }
                }
            }
        }
        advList.clear();
    }

    private StringBuilder getAdvMessageDataValue(Adv adv) {
        StringBuilder messageData = new StringBuilder();
        messageData.append(adv.getTime());
        return messageData;
    }

    private StringBuilder getAdvMessageDataKey(Adv adv) {
        StringBuilder messageData = new StringBuilder();
        messageData.append(adv.getId());
        return messageData;
    }

    private StringBuilder getClickMessageDataValue(Click click) {
        StringBuilder messageData = new StringBuilder();
        messageData.append(click.getTime());
        return messageData;
    }

    private StringBuilder getClickMessageDataKey(Click click) {
        StringBuilder messageData = new StringBuilder();
        messageData.append(click.getAdv().getId());
        return messageData;
    }

    private boolean clickSubmissionCondition(long value) {
        long submissionThreshold = (long) (clickProbability * advNum);
        return (value % submissionThreshold == submissionThreshold - 1);
    }

    @Override
    protected StringBuilder buildMessageData() {
        return null;
    }

    @Override
    protected void initialize() {
        initializeTopic();
        initializeSmallBufferProducer();
        initializeWorkloadData();
        initializeDataGenerators();
        initializeExecutorService();
    }

    @Override
    protected void initializeTopic() {
        TOPIC = null;
    }

    @Override
    protected void initializeDataGenerators() {
        generator = new RandomDataGenerator();
        generator.reSeed(10000L);
    }

    @Override
    protected void initializeWorkloadData() {
        clickProbability = Double.parseDouble(properties.getProperty("click.probability"));
        clickLambda = Double.parseDouble(properties.getProperty("click.lambda"));
        advNum = Long.parseLong(properties.getProperty("adv.num"));
    }
}
