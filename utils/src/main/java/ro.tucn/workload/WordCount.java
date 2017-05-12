package ro.tucn.workload;

import org.apache.log4j.Logger;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.frame.userfunctions.UserFunctions;
import ro.tucn.operator.Operator;
import ro.tucn.operator.OperatorCreator;
import ro.tucn.operator.PairOperator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Liviu on 4/15/2017.
 */
public class WordCount extends Workload {

    private static final Logger logger = Logger.getLogger(WordCount.class);

    public WordCount(OperatorCreator creator) throws WorkloadException {
        super(creator);
    }

    public void process() {
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>WORD COUNT<<<<<<<<<<<<<<<<<");
        Operator<String> wordOperators = getStringStreamOperator("source", "topic1");
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>1<<<<<<<<<<<<<<<<<");
        wordOperators.print();

        PairOperator<String, Integer> stringIntegerPairOperator = wordOperators.flatMapToPair((String s) -> {
            List<Tuple2<String, Integer>> list = new ArrayList<>();
            Arrays.asList(s.split(" ")).forEach(s1 -> list.add(new Tuple2<String, Integer>(s1, 1)));
            return list;
        });
        //logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>2<<<<<<<<<<<<<<<<<");
        stringIntegerPairOperator.print();
        //logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>3<<<<<<<<<<<<<<<<<");
        PairOperator<String, Integer> sum = stringIntegerPairOperator.reduceByKey(UserFunctions.sumReduce, "sum");
        //logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>4<<<<<<<<<<<<<<<<<");
        sum.print();
        //logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>5<<<<<<<<<<<<<<<<<");
        PairOperator<String, Integer> accumulate = sum.updateStateByKey((ReduceFunction<Integer>) UserFunctions.updateStateCount, "accumulate");
        //logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>6<<<<<<<<<<<<<<<<<");
        accumulate.print();
        //logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>7<<<<<<<<<<<<<<<<<");
    }
}
