package ro.tucn.generator.sender.offline;

import ro.tucn.generator.helper.entity.KMeansJSONHelper;
import ro.tucn.kMeans.Point;
import ro.tucn.util.Message;

/**
 * Created by Liviu on 6/28/2017.
 */
public class KMeansSenderOffline extends AbstractOfflineSender {

    private KMeansJSONHelper jsonHelper;

    public KMeansSenderOffline() {
        super();
        jsonHelper = new KMeansJSONHelper();
    }


    @Override
    public void send(Object o) {
        Point point = (Point) o;
        String key = jsonHelper.getMessageKey(point);
        String value = jsonHelper.getMessageValue(point);
        Message message = new Message(key, jsonHelper.toJson(point));
        String json = jsonHelper.toJson(message);
        send(topic, null, json);
    }
}
