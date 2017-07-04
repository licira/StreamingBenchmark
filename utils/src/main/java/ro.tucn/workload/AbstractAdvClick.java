package ro.tucn.workload;

import org.apache.log4j.Logger;
import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.DurationException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.operator.PairOperator;
import ro.tucn.util.TimeDuration;
import scala.Tuple2;

import java.util.concurrent.TimeUnit;

/**
 * Created by Liviu on 7/4/2017.
 */
public abstract class AbstractAdvClick extends AbstractWorkload {

    private static final Logger logger = Logger.getLogger(AbstractAdvClick.class);

    public AbstractAdvClick(ContextCreator creator) throws WorkloadException {
        super(creator);
    }

    public void process(PairOperator<String, String> advs, PairOperator<String, String> clicks, int streamWindowOne, int streamWindowTwo) {
        PairOperator<String, Tuple2<String, String>> advClick = null;
        try {
            advClick = advs.advClick(clicks,
                    new TimeDuration(TimeUnit.SECONDS, streamWindowOne),
                    new TimeDuration(TimeUnit.SECONDS, streamWindowTwo));
            advClick.print();
        } catch (WorkloadException e) {
            logger.error(e.getMessage());
        } catch (DurationException e) {
            logger.error(e.getMessage());
        }
    }
}
