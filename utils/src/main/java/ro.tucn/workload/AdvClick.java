package ro.tucn.workload;

import org.apache.log4j.Logger;
import ro.tucn.exceptions.DurationException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.frame.functions.AssignTimeFunction;
import ro.tucn.logger.SerializableLogger;
import ro.tucn.operator.OperatorCreator;
import ro.tucn.operator.PairOperator;
import ro.tucn.util.TimeDuration;
import scala.Tuple2;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Created by Liviu on 4/9/2017.
 */
public class AdvClick extends Workload {

    private static final Logger logger = SerializableLogger.getLogger(AdvClick.class);

    private int streamWindowOne;
    private int streamWindowTwo;

    public AdvClick(OperatorCreator creator) throws WorkloadException {
        super(creator);
        streamWindowOne = Integer.parseInt(properties.getProperty("stream1.window"));
        streamWindowTwo = Integer.parseInt(properties.getProperty("stream2.window"));
    }

    @Override
    public void process() {
        System.out.println("3");
        PairOperator<String, String> advs = getPairStreamOperator("adv", "topic1");
        PairOperator<String, String> clicks = getPairStreamOperator("click", "topic2");
        System.out.println("4");
        //advs.print();
        //clicks.print();
        PairOperator<String, Tuple2<String, String>> advClick = null;
        try {
            System.out.println("5");
            advClick = advs.join(clicks,
                    new TimeDuration(TimeUnit.SECONDS, streamWindowOne),
                    new TimeDuration(TimeUnit.SECONDS, streamWindowTwo));
            advClick.print();
            advClick.sink();
            System.out.println("6");
        } catch (WorkloadException e) {
            logger.error(e.getMessage());
        } catch (DurationException e) {
            logger.error(e.getMessage());
        }
    }

    private static class TimeAssigner implements AssignTimeFunction<Long>, Serializable {
        public long assign(Long var1) {
            return var1;
        }
    }
}