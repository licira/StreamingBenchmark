package ro.tucn.flink.consumer;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import ro.tucn.DataMode;
import ro.tucn.consumer.AbstractGeneratorConsumer;
import ro.tucn.flink.operator.batch.FlinkBatchOperator;
import ro.tucn.flink.operator.batch.FlinkBatchPairOperator;
import ro.tucn.generator.creator.GeneratorCreator;
import ro.tucn.generator.generator.AbstractGenerator;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.BatchOperator;
import ro.tucn.operator.BatchPairOperator;
import ro.tucn.util.Message;
import ro.tucn.util.TimeHolder;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Liviu on 6/27/2017.
 */
public class FlinkGeneratorConsumer extends AbstractGeneratorConsumer {

    private AbstractGenerator generator;
    private ExecutionEnvironment env;

    public FlinkGeneratorConsumer(ExecutionEnvironment env) {
        super();
        this.env = env;
        this.parallelism = env.getParallelism();
    }

    @Override
    public void askGeneratorToProduceData(String topic, int numberOfEntities) {
        generator = GeneratorCreator.getNewGenerator(topic, DataMode.BATCH, numberOfEntities);
        generator.generate(0, 0);
    }

    @Override
    public BatchPairOperator<String, String> getPairOperator(Properties properties,
                                                             String topicPropertyName) {
        String topic = getTopicFromProperties(properties, topicPropertyName);
        DataSet<String> dataSetWithJsonAsValue = getStringWithJsonAsValueDatasetFromGenerator(topic);
        DataSet<Tuple2<String, String>> pairDataSet = getPairDataSetFromDataSetWithJsonAsValue(dataSetWithJsonAsValue);
        return new FlinkBatchPairOperator<String, String>(pairDataSet, parallelism);
    }

    @Override
    public BatchOperator<Point> getPointOperator(Properties properties,
                                                 String topicPropertyName) {
        String topic = getTopicFromProperties(properties, topicPropertyName);
        DataSet<String> jsonDataSet = getStringDataSetFromGenerator(topic);
        DataSet<Point> pointDataSet = getPointDataSetFromJsonDataSet(jsonDataSet);
        return new FlinkBatchOperator<Point>(pointDataSet, parallelism);
    }

    @Override
    public BatchOperator<String> getStringOperator(Properties properties,
                                                   String topicPropertyName) {
        String topic = getTopicFromProperties(properties, topicPropertyName);
        DataSet<String> dataSetWithJsonAsValue = getStringWithJsonAsValueDataSetFromGenerator(topic);
        DataSet<String> dataSet = getStringDataSetFromDataSetWithJsonAsValue(dataSetWithJsonAsValue);
        return new FlinkBatchOperator<String>(dataSet, parallelism);
    }

    @Override
    public BatchOperator<TimeHolder<String>> getStringOperatorWithTimeHolder(Properties properties,
                                                                             String topicPropertyName) {
        return null;
    }

    private DataSet<Tuple2<String,String>> getPairDataSetFromDataSetWithJsonAsValue(DataSet<String> dataSetWithJsonAsValue) {
        return dataSetWithJsonAsValue.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                Gson gson = new Gson();
                Message msg = gson.fromJson(s, Message.class);
                return new Tuple2<String, String>(msg.getKey(), msg.getValue());
            }
        });
    }

    private DataSet<String> getStringWithJsonAsValueDatasetFromGenerator(String topic) {
        return getDataSetFromGenerator(topic);
    }

    private DataSet<String> getStringDataSetFromGenerator(String topic) {
        DataSet<String> dataSetWithJsonAsValue = getStringWithJsonAsValueDataSetFromGenerator(topic);
        return getStringDataSetFromDataSetWithJsonAsValue(dataSetWithJsonAsValue);
    }

    private DataSet<Point> getPointDataSetFromJsonDataSet(DataSet<String> jsonDataSet) {
        return jsonDataSet.map(new MapFunction<String, Point>() {
            @Override
            public Point map(String s) throws Exception {
                Gson gson = new Gson();
                Point point = gson.fromJson(s, Point.class);
                return point;
            }
        });
    }

    private DataSet<String> getStringDataSetFromDataSetWithJsonAsValue(DataSet<String> dataSetWithJsonAsValue) {
        return dataSetWithJsonAsValue.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                Gson gson = new Gson();
                Message msg = gson.fromJson(s, Message.class);
                return msg.getValue();
            }
        });
    }

    private DataSet<String> getStringWithJsonAsValueDataSetFromGenerator(String topic) {
        return getDataSetFromGenerator(topic);
    }

    private DataSet<String> getDataSetFromGenerator(String topic) {
        List<Map<String, String>> generatedData = generator.getGeneratedData(topic);
        List<String> jsons = getJsonListFromMapList(generatedData);
        return env.fromCollection(jsons);
    }

    private void setEnvParallelism(int parallelism) {
        env.setParallelism(parallelism);
    }
}
