package ro.tucn.generator.sender;

import ro.tucn.kMeans.Point;

import static ro.tucn.util.Topics.K_MEANS;

/**
 * Created by Liviu on 5/9/2017.
 */
public class KMeansSender extends AbstractMessageSender {

    private Point point;

    @Override
    public void send(Object o) {
        point = (Point) o;
        String messageValue = getMessageValue();
        String messageKey = getMessageKey();
        send(K_MEANS, messageKey, messageValue);
    }

    @Override
    protected String getMessageKey() {
        return Integer.toString(point.getId());
    }

    @Override
    protected String getMessageValue() {
        double[] location = point.getLocation();
        int locationSize = location.length;
        StringBuilder messageData = new StringBuilder();
        for(int i = 0; i < locationSize; i++) {
            messageData.append(location[i]);
            messageData.append(" ");
        }
        return messageData.toString();
    }
}
