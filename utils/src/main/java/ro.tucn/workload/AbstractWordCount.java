package ro.tucn.workload;

import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.operator.Operator;
import ro.tucn.operator.PairOperator;

/**
 * Created by Liviu on 7/4/2017.
 */
public abstract class AbstractWordCount extends AbstractWorkload {

    public AbstractWordCount(ContextCreator contextCreator) throws WorkloadException {
        super(contextCreator);
    }

    public void process(Operator<String> words) {
        PairOperator<String, Integer> countedWords = words.wordCount();
        countedWords.print();
    }
}