package ro.tucn.workload.batch;

import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.workload.Workload;

/**
 * Created by Liviu on 6/27/2017.
 */
public class WordCountFastBatch extends Workload {

    public WordCountFastBatch(ContextCreator contextCreator) throws WorkloadException {
        super(contextCreator);
    }

    @Override
    public void process() {

    }
}
