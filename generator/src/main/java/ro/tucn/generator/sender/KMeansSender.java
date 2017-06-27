package ro.tucn.generator.sender;

import ro.tucn.generator.helper.KMeansJSONHelper;
import ro.tucn.generator.helper.SentenceJSONHelper;
import ro.tucn.kMeans.Point;
import ro.tucn.util.Message;

/**
 * Created by Liviu on 5/9/2017.
 */
public class KMeansSender extends AbstractMessageSender {

    private KMeansJSONHelper jsonHelper;

    public KMeansSender() {
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
