package ro.tucn.workload;

import org.apache.log4j.Logger;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.operator.Operator;
import ro.tucn.operator.OperatorCreator;
import ro.tucn.operator.PairOperator;

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
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>WORD COUNT FAST<<<<<<<<<<<<<<<<<");
        Operator<String> wordOperators = getStringStreamOperator("source", "topic1");
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>1<<<<<<<<<<<<<<<<<");
        wordOperators.print();
        PairOperator<String, Integer> stringIntegerPairOperator = wordOperators.flatMapToPair();
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>2<<<<<<<<<<<<<<<<<");
        stringIntegerPairOperator.print();
    }
}
