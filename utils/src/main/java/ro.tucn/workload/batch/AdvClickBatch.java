package ro.tucn.workload.batch;

import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.workload.Workload;

import java.lang.reflect.InvocationTargetException;

/**
 * Created by Liviu on 6/27/2017.
 */
public class AdvClickBatch extends Workload {

    public AdvClickBatch(ContextCreator contextCreator) throws WorkloadException {
        super(contextCreator);
    }

    @Override
    public void process() throws WorkloadException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

    }
}
