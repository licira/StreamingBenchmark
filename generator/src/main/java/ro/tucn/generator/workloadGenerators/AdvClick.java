package ro.tucn.generator.workloadGenerators;

import org.apache.commons.math3.random.RandomDataGenerator;
import ro.tucn.util.Topics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Liviu on 4/4/2017.
 */
public class AdvClick extends Generator {

    private RandomDataGenerator generator;
    private ExecutorService cachedPool;

    private static String ADV_TOPIC = Topics.ADV;
    private static String CLICK_TOPIC = Topics.CLICK;
    private static long ADV_NUM = 10;
    private double clickLambda;
    private double clickProbability;

    public AdvClick() {
        super();
        initialize();
    }

    @Override
    public void generate(int sleepFrequency) {
        ArrayList<Advertisement> advList = new ArrayList();

        initializePerformanceLogWithCurrentTime();
        performanceLog.disablePrint();
        for (long i = 0; i < ADV_NUM; ++i) {
            // advertisement id
            String advId = UUID.randomUUID().toString();

            long timestamp = System.nanoTime();
            send(ADV_TOPIC, advId, String.format("%d\t%s", timestamp, advId));

            // whether customer clicked this advertisement
            if (generator.nextUniform(0, 1) <= clickProbability) {
                // long deltaT = (long)ro.tucn.generator.nextExponential(clickLambda) * 1000;
                long deltaT = (long) generator.nextGaussian(clickLambda, 1) * 1000;
                // System.out.println(deltaT);
                advList.add(new Advertisement(advId, timestamp + deltaT));
            }
            if (i % 100 == 0) {
                cachedPool.submit(new ClickThread(advList));
                advList = new ArrayList();
            }
            performanceLog.logThroughputAndLatency(System.nanoTime());
            // control data generate speed
            if (sleepFrequency > 0 && i % sleepFrequency == 0) {
                //Thread.sleep(1);
            }
        }
        performanceLog.logTotalThroughputAndTotalLatency();
        cachedPool.shutdown();
        try {
            cachedPool.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.info(e.getMessage());
        }
        producer.close();
    }

    @Override
    protected void initialize() {
        initializeTopic();
        initializeSmallBufferProducer();
        initializeWorkloadData();
        initializeDataGenerators();
        initializeExecutorService();
    }

    private void initializeExecutorService() {
        // Obtain a cached thread pool
        ExecutorService cachedPool = Executors.newCachedThreadPool();
        // sub thread use variable in main thread
        // for loop to generate advertisement
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
    }

    private static class Advertisement implements Comparable<Advertisement> {
        private String id;
        private long time;

        Advertisement(String id, long time) {
            this.id = id;
            this.time = time;
        }

        @Override
        public int compareTo(Advertisement o) {
            if (this.time > o.time)
                return 1;
            else if (this.time == o.time)
                return 0;
            else
                return -1;
        }
    }

    private class ClickThread implements Runnable {
        private ArrayList<Advertisement> advList;

        public ClickThread(ArrayList<Advertisement> advList) {
            this.advList = advList;
            Collections.sort(this.advList);
        }

        @Override
        public void run() {
            for (Advertisement adv : advList) {
                if (System.nanoTime() < adv.time) {
                    try {
                        Thread.sleep(adv.time - System.nanoTime());
                    } catch (InterruptedException e) {
                        logger.error(e.getMessage());
                    }
                }
                send(CLICK_TOPIC, adv.id, String.format("%d\t%s", System.nanoTime(), adv.id));
                // System.out.println("Clicked: " + adv.id);
            }
        }
    }
}
