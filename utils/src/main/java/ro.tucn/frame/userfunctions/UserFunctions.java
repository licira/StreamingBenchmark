package ro.tucn.frame.userfunctions;

import org.apache.log4j.Logger;
import ro.tucn.frame.functions.FlatMapFunction;
import ro.tucn.frame.functions.MapPairFunction;
import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.util.WithTime;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Liviu on 4/8/2017.
 */
public class UserFunctions implements Serializable {

    private static final Logger logger = Logger.getLogger(UserFunctions.class);

    public static FlatMapFunction<WithTime<String>, WithTime<String>> splitFlatMapWithTime
            = new FlatMapFunction<WithTime<String>, WithTime<String>>() {
        public Iterator<WithTime<String>> flatMap(WithTime<String> var1) throws Exception {
            List<WithTime<String>> list = new ArrayList();
            for (String str : var1.getValue().toLowerCase().split("\\W+")) {
                list.add(new WithTime(str, var1.getTime()));
            }
            return (Iterator<WithTime<String>>) list;
        }
    };
    public static MapPairFunction<WithTime<String>, String, WithTime<Integer>> mapToStrIntPairWithTime
            = new MapPairFunction<WithTime<String>, String, WithTime<Integer>>() {
        public Tuple2<String, WithTime<Integer>> mapToPair(WithTime<String> s) {
            return new Tuple2(s.getValue(), new WithTime(1, s.getTime()));
        }
    };

    public static ReduceFunction<WithTime<Integer>> sumReduceWithTime2 = new ReduceFunction<WithTime<Integer>>() {
        public WithTime<Integer> reduce(WithTime<Integer> var1, WithTime<Integer> var2) throws Exception {
            return new WithTime(var1.getValue() + var2.getValue(), Math.max(var1.getTime(), var2.getTime()));
        }
    };
}