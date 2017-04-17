package ro.tucn.flink.function;

import org.apache.flink.api.common.functions.MapFunction;
import ro.tucn.frame.functions.MapWithInitListFunction;

import java.util.List;

/**
 * Created by Liviu on 4/17/2017.
 */
public class MapFunctionWithInitList<T, R> implements MapFunction<T, R> {

    private List<T> initList;
    private MapWithInitListFunction<T, R> function;

    public MapFunctionWithInitList(MapWithInitListFunction<T, R> function, List<T> initList) {
        this.function = function;
        this.initList = initList;
    }

    @Override
    public R map(T t) throws Exception {
        return null;
    }
}
