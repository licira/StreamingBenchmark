package ro.tucn.workload;

import org.apache.log4j.Logger;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.frame.userfunctions.UserFunctions;
import ro.tucn.operator.Operator;
import ro.tucn.operator.OperatorCreator;
import ro.tucn.operator.PairOperator;
import ro.tucn.util.WithTime;

import java.lang.reflect.InvocationTargetException;

/**
 * Created by Liviu on 4/15/2017.
 */
public class WordCountFast extends Workload {

    private static final Logger logger = Logger.getLogger(WordCount.class);

    public WordCountFast(OperatorCreator creator) throws WorkloadException {
        super(creator);
    }

    @Override
    public void process() throws WorkloadException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        try {
            Operator<String> wordOperators = getStringStreamOperator("adv", "topic1");
            PairOperator<String, WithTime<Integer>> counts =
                    wordOperators.flatMapToPair(UserFunctions.flatMapToPairAddTime, "splitter")
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
