package ro.tucn.generator.sender;

import ro.tucn.kMeans.Point;
import ro.tucn.util.Message;

/**
 * Created by Liviu on 5/9/2017.
 */
public class KMeansSender extends AbstractMessageSender {

    @Override
    public void send(Object o) {
        Point point = (Point) o;
        String key = getMessageKey(point);
        String value = getMessageValue(point);
        Message message = new Message(key, toJson(point));
        String json = toJson(message);
        send(topic, null, json);
    }

    private String getMessageKey(Point point) {
        return Integer.toString(point.getId());
    }

    private String getMessageValue(Point point) {
        double[] location = point.getCoordinates();
        int locationSize = location.length;
        StringBuilder messageData = new StringBuilder();
        int i;
        for (i = 0; i < locationSize - 1; i++) {
            messageData.append(location[i]);
            messageData.append(" ");
        }
        messageData.append(location[i]);
        return toJson(point);
    }
}
