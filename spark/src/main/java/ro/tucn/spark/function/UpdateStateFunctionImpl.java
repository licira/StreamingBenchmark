package ro.tucn.spark.function;

import com.google.common.base.Optional;
import org.apache.log4j.Logger;
import ro.tucn.logger.SerializableLogger;
import org.apache.spark.api.java.function.Function2;
import ro.tucn.frame.functions.ReduceFunction;

import java.util.List;

/**
 * Created by Liviu on 4/8/2017.
 */
public class UpdateStateFunctionImpl<V> implements Function2<List<V>, Optional<V>, Optional<V>> {

    private static final long serialVersionUID = -7713561370480802413L;
    private static Logger logger = Logger.getLogger(ReduceFunctionImpl.class.getSimpleName());

    private ReduceFunction<V> fun;

    public UpdateStateFunctionImpl(ReduceFunction<V> function) {
        this.fun = function;
    }

    public Optional<V> call(List<V> values, Optional<V> vOptional) throws Exception {
        V reducedValue = vOptional.orNull();
        for (V value : values) {
            if (null == reducedValue) {
                reducedValue = value;
            } else {
                reducedValue = fun.reduce(reducedValue, value);
            }
        }
        if (null != reducedValue)
            return Optional.of(reducedValue);
        return vOptional;
    }
}
