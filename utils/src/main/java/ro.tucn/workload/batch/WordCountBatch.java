package ro.tucn.workload.batch;

import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.workload.Workload;

/**
 * Created by Liviu on 6/27/2017.
 */
public class WordCountBatch extends Workload {

    public WordCountBatch(ContextCreator contextCreator) throws WorkloadException {
        super(contextCreator);
    }

    @Override
    public void process() {

    }
}
