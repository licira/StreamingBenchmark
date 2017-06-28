package ro.tucn.consumer;

import org.apache.log4j.Logger;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.BatchOperator;
import ro.tucn.operator.BatchPairOperator;
import ro.tucn.util.TimeHolder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Liviu on 6/27/2017.
 */
public abstract class AbstractGeneratorConsumer extends AbstractConsumer {

    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    public abstract BatchOperator<String> getStringOperator(Properties properties,
                                                            String topicPropertyName);

    public abstract BatchPairOperator<String, String> getPairOperator(Properties properties,
                                                                      String topicPropertyName);

    public abstract BatchOperator<Point> getPointOperator(Properties properties,
                                                          String topicPropertyName);

    public abstract void askGeneratorToProduceData(String topic);

    public abstract BatchOperator<TimeHolder<String>> getStringOperatorWithTimeHolder(Properties properties,
                                                                                      String topicPropertyName);

    protected List<String> getJsonListFromMapList(List<Map<String, String>> data ) {
        List<String> jsons = new ArrayList<>();
        try {
            for (Map<String, String> map : data) {
                String json = map.get(null);
                jsons.add(json);
            }
        } catch (NullPointerException e) {
            logger.error(e.getMessage());
        }
        return jsons;
    }

    protected String getTopicFromProperties(Properties properties, String topicPropertyName) {
        return properties.getProperty(topicPropertyName);
    }
}
