package ro.tucn.frame.functions;

import com.google.common.base.Optional;

import java.util.List;

/**
 * Created by Liviu on 4/8/2017.
 */
public interface UpdateStateFunction<T> {
    Optional<T> update(List<T> values, Optional<T> cumulateValue);
}

