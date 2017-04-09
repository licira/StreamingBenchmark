package ro.tucn.spark.statistics;

import org.apache.spark.streaming.scheduler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Liviu on 8/4/17.
 */
public class PerformanceStreamingListener implements StreamingListener {

    private static Logger logger = LoggerFactory.getLogger(PerformanceStreamingListener.class);


    /**
     * Called when a receiver has been started
     */
    public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {
    }

    /**
     * Called when a receiver has reported an error
     */
    public void onReceiverError(StreamingListenerReceiverError receiverError) {
    }

    /**
     * Called when a receiver has been stopped
     */
    public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {
    }

    /**
     * Called when a batch of jobs has been submitted for processing.
     */
    public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {
    }

    /**
     * Called when processing of a batch of jobs has started.
     */
    public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {
    }

    /**
     * Called when processing of a batch of jobs has completed.
     */
    public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
        BatchInfo batchInfo = batchCompleted.batchInfo();
        logger.warn(batchInfo.batchTime().milliseconds() + "\t"
                + batchInfo.schedulingDelay().get() + "\t"
                + batchInfo.processingDelay().get() + "\t"
                + batchInfo.totalDelay().get() + "\t"
                + batchInfo.numRecords());
    }
}