package ro.tucn.generator.workloadGenerators;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import ro.tucn.logger.SerializableLogger;
import ro.tucn.logger.SerializableLogger;
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

    private static KafkaProducer<String, String> producer;
    private static String ADV_TOPIC = Topics.ADV;
    private static String CLICK_TOPIC = Topics.CLICK;
    private static long ADV_NUM = 1000;
    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());
    private double clickLambda;
    private double clickProbability;

    public AdvClick() {
        super();
        producer = createSmallBufferProducer();
        initializeWorkloadData();
    }

    public void generate(int sleepFrequency) throws InterruptedException {
        //ThroughputLog throughput = new ThroughputLog(this.getClass().getSimpleName());
        long time = System.currentTimeMillis();

        // Obtain a cached thread pool
        ExecutorService cachedPool = Executors.newCachedThreadPool();

        RandomDataGenerator generator = new RandomDataGenerator();
        generator.reSeed(10000L);
        // sub thread use variable in main thread
        // for loop to generate advertisement

        ArrayList<Advertisement> advList = new ArrayList();
        for (long i = 1; i < ADV_NUM; ++i) {
            // advertisement id
            String advId = UUID.randomUUID().toString();
            long timestamp = System.currentTimeMillis();
            producer.send(new ProducerRecord<String, String>(ADV_TOPIC, advId, String.format("%d\t%s", timestamp, advId)));
            // System.out.println("Shown: " + System.currentTimeMillis() + "\t" + advId);

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

            //throughput.execute();
            // control data generate speed
            if (sleepFrequency > 0 && i % sleepFrequency == 0) {
                //Thread.sleep(1);
            }
        }
        cachedPool.shutdown();
        try {
            cachedPool.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.info(e.getMessage());
        }
        logger.info("LatencyLog: " + String.valueOf(System.currentTimeMillis() - time));
    }

    protected void initializeWorkloadData() {
        clickProbability = Double.parseDouble(properties.getProperty("click.probability", "0.3"));
        clickLambda = Double.parseDouble(properties.getProperty("click.lambda", "10"));
    }

    /*
    public static void main(String[] args) throws InterruptedException {
        int SLEEP_FREQUENCY = -1;
        if (args.length > 0) {
            SLEEP_FREQUENCY = Integer.parseInt(args[0]);
        }
        new AdvClick().generate(SLEEP_FREQUENCY);
        System.out.println("DONE");
    }
    */
    private static class Advertisement implements Comparable<Advertisement> {
        String id;
        long time;

        Advertisement(String id, long time) {
            this.id = id;
            this.time = time;
        }

        //@Override
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

        //@Override
        public void run() {
            for (Advertisement adv : advList) {
                if (System.currentTimeMillis() < adv.time) {
                    try {
                        Thread.sleep(adv.time - System.currentTimeMillis());
                    } catch (InterruptedException e) {
                        logger.error(e.getMessage());
                    }
                }
                producer.send(new ProducerRecord(CLICK_TOPIC, adv.id,
                        String.format("%d\t%s", System.currentTimeMillis(), adv.id)));
                // System.out.println("Clicked: " + adv.id);
            }
        }
    }
}
