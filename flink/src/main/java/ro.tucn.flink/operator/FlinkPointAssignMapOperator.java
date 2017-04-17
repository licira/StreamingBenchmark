package ro.tucn.flink.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Created by Liviu on 4/17/2017.
 */
public class FlinkPointAssignMapOperator<IN, OUT>
        extends AbstractUdfStreamOperator<OUT, MapFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT> {

    public FlinkPointAssignMapOperator(MapFunction<IN, OUT> userFunction) {
        super(userFunction);
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void processElement(StreamRecord<IN> streamRecord) throws Exception {
        OUT out = userFunction.map(streamRecord.getValue());
        if (null != out) {
            output.collect(streamRecord.replace(out));
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        output.emitWatermark(mark);
    }
}