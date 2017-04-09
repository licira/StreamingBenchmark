package ro.tucn.workload;

import org.apache.log4j.Logger;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.frame.functions.AssignTimeFunction;
import ro.tucn.frame.userfunctions.UserFunctions;
import ro.tucn.operator.OperatorCreator;
import ro.tucn.operator.PairWorkloadOperator;
import ro.tucn.util.TimeDuration;
import scala.Tuple2;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;

/**
 * Created by Liviu on 4/9/2017.
 */
public class AdvClick extends Workload {

    private final Logger logger = Logger.getLogger(this.getClass());

    private int streamWindowOne;
    private int streamWindowTwo;

    public AdvClick(OperatorCreator creator) throws WorkloadException {
        super(creator);
        streamWindowOne = Integer.parseInt(properties.getProperty("stream1.window"));
        streamWindowTwo = Integer.parseInt(properties.getProperty("stream2.window"));
    }

    @Override
    public void Process() throws WorkloadException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        try {

            PairWorkloadOperator<String, Long> advertisements = kafkaStreamOperator("advertisement")
                    .mapToPair(UserFunctions.mapToStringLongPair, "Extractor");
            PairWorkloadOperator<String, Long> clicks = kafkaStreamOperator2("click")
                    .mapToPair(UserFunctions.mapToStringLongPair, "Extractor2");
//            advertisements.print();
//            clicks.print();
            PairWorkloadOperator<String, Tuple2<Long, Long>> clicksWithCreateTime = advertisements.join(
                    "Join",
                    clicks,
                    new TimeDuration(TimeUnit.SECONDS, streamWindowOne),
                    new TimeDuration(TimeUnit.SECONDS, streamWindowTwo));

//            PairWorkloadOperator<String, Tuple2<Long, Long>> clicksWithCreateTime = advertisements.join(
//                    "Join",
//                    clicks,
//                    new TimeDurations(TimeUnit.SECONDS, stream1Window),
//                    new TimeDurations(TimeUnit.SECONDS, stream2Window),
//                    new TimeAssigner(),
//                    new TimeAssigner());

            clicksWithCreateTime.mapValue(UserFunctions.mapToWithTime, "MapToWithTime")
                    .sink();
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private static class TimeAssigner implements AssignTimeFunction<Long>, Serializable {
        public long assign(Long var1) {
            return var1;
        }
    }
}