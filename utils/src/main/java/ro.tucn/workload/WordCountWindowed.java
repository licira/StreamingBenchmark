package ro.tucn.workload;

import org.apache.log4j.Logger;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.frame.userfunctions.UserFunctions;
import ro.tucn.operator.OperatorCreator;
import ro.tucn.operator.PairWorkloadOperator;
import ro.tucn.operator.WorkloadOperator;
import ro.tucn.util.TimeDuration;
import ro.tucn.util.WithTime;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;

/**
 * Created by Liviu on 4/15/2017.
 */
public class WordCountWindowed extends Workload {

    private static final Logger logger = Logger.getLogger(WordCountWindowed.class);

    public WordCountWindowed(OperatorCreator creator) throws WorkloadException {
        super(creator);
    }

    @Override
    public void process() throws WorkloadException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        try {
            // Flink doesn't support shuffle().window()
            // Actually Flink does keyGrouping().window().update()
            // It is the same situation to Spark streaming
            WorkloadOperator<WithTime<String>> operator = stringStreamWithTime("source");
            PairWorkloadOperator<String, WithTime<Integer>> counts =
                    operator.flatMap(UserFunctions.splitFlatMapWithTime, "splitter")
                            .mapToPair(UserFunctions.mapToStrIntPairWithTime, "pair")
                            .reduceByKeyAndWindow(UserFunctions.sumReduceWithTime2, "counter",
                                    new TimeDuration(TimeUnit.SECONDS, 1), new TimeDuration(TimeUnit.SECONDS, 1));
            counts.sink();
            //cumulate counts
            //PairWorkloadOperator<String, Integer> cumulateCounts =counts.updateStateByKey(UserFunctions.sumReduce, "cumulate");
            //cumulateCounts.print();
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}