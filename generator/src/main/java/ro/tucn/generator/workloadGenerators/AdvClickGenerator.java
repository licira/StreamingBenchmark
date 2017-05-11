package ro.tucn.generator.workloadGenerators;

import org.apache.commons.math3.random.RandomDataGenerator;
import ro.tucn.generator.entity.Adv;
import ro.tucn.generator.entity.Click;
import ro.tucn.generator.helper.AdvHelper;
import ro.tucn.generator.helper.ClickHelper;
import ro.tucn.generator.helper.TimeHelper;
import ro.tucn.generator.sender.AbstractMessageSender;
import ro.tucn.generator.sender.AdvSender;
import ro.tucn.generator.sender.ClickSender;
import ro.tucn.util.Topics;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Liviu on 4/4/2017.
 */
public class AdvClickGenerator extends AbstractGenerator {

    public static String ADV_TOPIC = Topics.ADV;
    public static String CLICK_TOPIC = Topics.CLICK;

    private AbstractMessageSender advSender;
    private AbstractMessageSender clickSender;
    private RandomDataGenerator generator;
    private ExecutorService cachedPool;

    private ArrayList<Adv> advs;
    private static Long advNum;
    private double clickLambda;
    private double clickProbability;

    public AdvClickGenerator() {
        super();
        initialize();
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
        long deltaT = (long) generator.nextGaussian(clickLambda, 1) * advNum;
        adv.setTimestamp(adv.getTimestamp() + deltaT);
        advs.add(adv);
    }

    private void initializeExecutorService() {
        // Obtain a cached thread pool
        cachedPool = Executors.newCachedThreadPool();
        // sub thread use variable in main thread
        // for loop to generate advertisement
    }

    private void initializeMessageSendersWithSmallBuffers() {
        advSender = new AdvSender();
        advSender.initializeSmallBufferProducer(bootstrapServers);
        clickSender = new ClickSender();
        clickSender.initializeSmallBufferProducer(bootstrapServers);
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
        Adv adv = AdvHelper.getNewAdv();
        advSender.send(adv);
        return adv;
    }

    private void submitNewClick() {
        for (Adv adv : advs) {
            // probability that the customer would click this advertisement
            if (generator.nextUniform(0, 1) <= clickProbability) {
                Click click = ClickHelper.createNewClick(adv);
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
        long submissionThreshold = (long) (clickProbability * advNum);
        return (value % submissionThreshold == submissionThreshold - 1);
    }

    @Override
    protected void submitData(int sleepFrequency) {
        advs = new ArrayList();
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

    @Override
    protected void initialize() {
        initializeMessageSendersWithSmallBuffers();
        initializeWorkloadData();
        initializeDataGenerators();
        initializeExecutorService();
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