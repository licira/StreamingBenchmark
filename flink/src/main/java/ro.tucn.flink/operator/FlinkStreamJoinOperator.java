package ro.tucn.flink.operator;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.shaded.com.google.common.cache.Cache;
import org.apache.flink.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.MultiplexingStreamRecordSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Created by Liviu on 4/17/2017.
 */
public class FlinkStreamJoinOperator<K, IN1, IN2, OUT>
        extends AbstractUdfStreamOperator<OUT, JoinFunction<IN1, IN2, OUT>>
        implements TwoInputStreamOperator<IN1, IN2, OUT> {

    private static final Logger logger = LoggerFactory.getLogger(FlinkStreamJoinOperator.class);

    //private HeapWindowBuffer<IN1> stream1Buffer;
    //private HeapWindowBuffer<IN2> stream2Buffer;
    private final KeySelector<IN1, K> keySelector1;
    private final KeySelector<IN2, K> keySelector2;
    protected transient long currentWatermark1 = -1L;
    protected transient long currentWatermark2 = -1L;
    protected transient long currentWatermark = -1L;
    private Cache<K, StreamRecord<IN1>> stream1Buffer;
    private Cache<K, StreamRecord<IN2>> stream2Buffer;
    private long stream1WindowLength;
    private long stream2WindowLength;
    private TypeSerializer<IN1> inputSerializer1;
    private TypeSerializer<IN2> inputSerializer2;
    /**
     * If this is true. The current processing time is set as the timestamp of incoming elements.
     * This for use with a {@link org.apache.flink.streaming.api.windowing.evictors.TimeEvictor}
     * if eviction should happen based on processing time.
     */
    private boolean setProcessingTime = false;

    public FlinkStreamJoinOperator(JoinFunction<IN1, IN2, OUT> userFunction,
                                   KeySelector<IN1, K> keySelector1,
                                   KeySelector<IN2, K> keySelector2,
                                   long stream1WindowLength,
                                   long stream2WindowLength,
                                   TypeSerializer<IN1> inputSerializer1,
                                   TypeSerializer<IN2> inputSerializer2) {
        super(userFunction);
        this.keySelector1 = requireNonNull(keySelector1);
        this.keySelector2 = requireNonNull(keySelector2);

        this.stream1WindowLength = requireNonNull(stream1WindowLength);
        this.stream2WindowLength = requireNonNull(stream2WindowLength);

        this.inputSerializer1 = requireNonNull(inputSerializer1);
        this.inputSerializer2 = requireNonNull(inputSerializer2);
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (null == inputSerializer1 || null == inputSerializer2) {
            throw new IllegalStateException("Input serializer was not set.");
        }

//        this.stream1Buffer = new HeapWindowBuffer.Factory<IN1>().create();
//        this.stream2Buffer = new HeapWindowBuffer.Factory<IN2>().create();
        stream1Buffer = CacheBuilder.newBuilder()
                .expireAfterWrite(stream1WindowLength, TimeUnit.MILLISECONDS)
                .build();
        stream2Buffer = CacheBuilder.newBuilder()
                .expireAfterWrite(stream2WindowLength, TimeUnit.MILLISECONDS)
                .build();

    }

    /**
     * @param element record of stream1
     * @throws Exception
     */
    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        if (setProcessingTime) {
            element.replace(element.getValue(), System.currentTimeMillis());
        }

        if (setProcessingTime) {
            IN1 item1 = element.getValue();
            long time1 = element.getTimestamp();

            StreamRecord<IN2> record = stream2Buffer.getIfPresent(keySelector1.getKey(element.getValue()));
            if (record != null) {
                output.collect(new StreamRecord<>(userFunction.join(item1, record.getValue())));
            } else {
                stream1Buffer.put(keySelector1.getKey(element.getValue()), element);
            }
        } else {
            stream1Buffer.put(keySelector1.getKey(element.getValue()), element);
        }
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        if (setProcessingTime) {
            element.replace(element.getValue(), System.currentTimeMillis());
        }

        if (setProcessingTime) {
            IN2 item2 = element.getValue();
            long time2 = element.getTimestamp();

            StreamRecord<IN1> record = stream1Buffer.getIfPresent(keySelector2.getKey(element.getValue()));
            if (record != null) {
                output.collect(new StreamRecord<>(userFunction.join(record.getValue(), item2)));
            } else {
                stream2Buffer.put(keySelector2.getKey(element.getValue()), element);
            }
        } else {
            stream2Buffer.put(keySelector2.getKey(element.getValue()), element);
        }
    }

    /**
     * Process join operator on element during [currentWaterMark, watermark)
     *
     * @param watermark
     * @throws Exception
     */
    private void processWatermark(long watermark) throws Exception {
        if (setProcessingTime)
            return;
        // process elements after currentWatermark and lower than watermark
        for (Map.Entry<K, StreamRecord<IN1>> record1 : stream1Buffer.asMap().entrySet()) {
            if (record1.getValue().getTimestamp() >= this.currentWatermark
                    && record1.getValue().getTimestamp() < watermark) {
                StreamRecord<IN2> record2 = stream2Buffer.getIfPresent(record1.getKey());
                if (record2 != null) {
                    stream1Buffer.invalidate(record1.getKey());
                    stream2Buffer.invalidate(record1.getKey());
                    output.collect(new StreamRecord<OUT>(
                            userFunction.join(record1.getValue().getValue(), record2.getValue()))
                    );
                }
            }
        }

        for (Map.Entry<K, StreamRecord<IN2>> record2 : stream2Buffer.asMap().entrySet()) {
            if (record2.getValue().getTimestamp() >= this.currentWatermark
                    && record2.getValue().getTimestamp() < watermark) {
                StreamRecord<IN1> record1 = stream1Buffer.getIfPresent(record2.getKey());
                if (record1 != null) {
                    stream1Buffer.invalidate(record2.getKey());
                    stream2Buffer.invalidate(record2.getKey());
                    output.collect(new StreamRecord<OUT>(
                            userFunction.join(record1.getValue(), record2.getValue().getValue()))
                    );
                }
            }
        }
    }

    @Override
    public void processWatermark1(Watermark mark) throws Exception {

        long watermark = Math.min(mark.getTimestamp(), currentWatermark2);
        // process elements [currentWatermark, watermark)
        processWatermark(watermark);

        output.emitWatermark(mark);
        this.currentWatermark = watermark;
        this.currentWatermark1 = mark.getTimestamp();
    }

    @Override
    public void processWatermark2(Watermark mark) throws Exception {

        long watermark = Math.min(mark.getTimestamp(), currentWatermark1);
        // process elements [currentWatermark, watermark)
        processWatermark(watermark);

        output.emitWatermark(mark);
        this.currentWatermark = watermark;
        this.currentWatermark2 = mark.getTimestamp();
    }

    /**
     * When this flag is enabled the current processing time is set as the timestamp of elements
     * upon arrival. This must be used, for example, when using the
     * {@link org.apache.flink.streaming.api.windowing.evictors.TimeEvictor} with processing
     * time semantics.
     */
    public FlinkStreamJoinOperator<K, IN1, IN2, OUT> enableSetProcessingTime(boolean setProcessingTime) {
        this.setProcessingTime = setProcessingTime;
        return this;
    }

    // ------------------------------------------------------------------------
    //  checkpointing and recovery
    // ------------------------------------------------------------------------

    @Override
    public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
        StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

        // we write the panes with the key/value maps into the stream
        StateBackend.CheckpointStateOutputView out = getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);

        out.writeLong(stream1WindowLength);
        out.writeLong(stream2WindowLength);

        MultiplexingStreamRecordSerializer<IN1> recordSerializer1 = new MultiplexingStreamRecordSerializer<>(inputSerializer1);
        out.writeLong(stream1Buffer.size());
        for (StreamRecord<IN1> element : stream1Buffer.asMap().values()) {
            recordSerializer1.serialize(element, out);
        }

        MultiplexingStreamRecordSerializer<IN2> recordSerializer2 = new MultiplexingStreamRecordSerializer<>(inputSerializer2);
        out.writeLong(stream2Buffer.size());
        for (StreamRecord<IN2> element : stream2Buffer.asMap().values()) {
            recordSerializer2.serialize(element, out);
        }

        taskState.setOperatorState(out.closeAndGetHandle());
        return taskState;
    }

    @Override
    public void restoreState(StreamTaskState taskState) throws Exception {
        super.restoreState(taskState);

        final ClassLoader userClassloader = getUserCodeClassloader();

        @SuppressWarnings("unchecked")
        StateHandle<DataInputView> inputState = (StateHandle<DataInputView>) taskState.getOperatorState();
        DataInputView in = inputState.getState(userClassloader);

        stream1WindowLength = in.readLong();
        stream2WindowLength = in.readLong();

        long numElements = in.readLong();

        MultiplexingStreamRecordSerializer<IN1> recordSerializer1 = new MultiplexingStreamRecordSerializer<>(inputSerializer1);
        for (int i = 0; i < numElements; i++) {
            StreamRecord<IN1> record = recordSerializer1.deserialize(in).asRecord();
            stream1Buffer.put(keySelector1.getKey(record.getValue()), record);
        }

        long numElements2 = in.readLong();
        MultiplexingStreamRecordSerializer<IN2> recordSerializer2 = new MultiplexingStreamRecordSerializer<>(inputSerializer2);
        for (int i = 0; i < numElements2; i++) {
            StreamRecord<IN2> record = recordSerializer2.deserialize(in).asRecord();
            stream2Buffer.put(keySelector2.getKey(record.getValue()), record);
        }
    }
}
