package ro.tucn.frame.userfunctions;

import org.apache.log4j.Logger;
import ro.tucn.frame.functions.FlatMapFunction;
import ro.tucn.frame.functions.MapPairFunction;
import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.util.TimeHolder;
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

    public static FlatMapFunction<TimeHolder<String>, TimeHolder<String>> splitFlatMapTimeHolder
            = new FlatMapFunction<TimeHolder<String>, TimeHolder<String>>() {
        public Iterator<TimeHolder<String>> flatMap(TimeHolder<String> var1) throws Exception {
            List<TimeHolder<String>> list = new ArrayList();
            for (String str : var1.getValue().toLowerCase().split("\\W+")) {
                list.add(new TimeHolder(str, var1.getTime()));
            }
            return (Iterator<TimeHolder<String>>) list;
        }
    };
    public static MapPairFunction<TimeHolder<String>, String, TimeHolder<Integer>> mapToStrIntPairTimeHolder
            = new MapPairFunction<TimeHolder<String>, String, TimeHolder<Integer>>() {
        public Tuple2<String, TimeHolder<Integer>> mapToPair(TimeHolder<String> s) {
            return new Tuple2(s.getValue(), new TimeHolder(1, s.getTime()));
        }
    };

    public static ReduceFunction<TimeHolder<Integer>> sumReduceTimeHolder2 = new ReduceFunction<TimeHolder<Integer>>() {
        public TimeHolder<Integer> reduce(TimeHolder<Integer> var1, TimeHolder<Integer> var2) throws Exception {
            return new TimeHolder(var1.getValue() + var2.getValue(), Math.max(var1.getTime(), var2.getTime()));
        }
    };
}