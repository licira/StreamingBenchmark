package ro.tucn.workload;

import org.apache.log4j.Logger;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.frame.userfunctions.UserFunctions;
import ro.tucn.operator.OperatorCreator;
import ro.tucn.operator.PairOperator;
import ro.tucn.operator.Operator;
import ro.tucn.util.WithTime;

import java.lang.reflect.InvocationTargetException;

/**
 * Created by Liviu on 4/15/2017.
 */
public class WordCount extends Workload {

    private static final Logger logger = Logger.getLogger(WordCount.class);

    public WordCount(OperatorCreator creator) throws WorkloadException {
        super(creator);
    }

    public void process() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        try {
            Operator<WithTime<String>> operator = stringStreamWithTime("source", "topic1");
            PairOperator<String, WithTime<Integer>> counts =
                    operator.flatMap(UserFunctions.splitFlatMapWithTime, "splitter")
                            .mapToPair(UserFunctions.mapToStrIntPairWithTime, "pair")
                            .reduceByKey(UserFunctions.sumReduceWithTime, "sum")
                            .updateStateByKey(UserFunctions.sumReduceWithTime, "accumulate");
            counts.sink();
            counts.print();
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
