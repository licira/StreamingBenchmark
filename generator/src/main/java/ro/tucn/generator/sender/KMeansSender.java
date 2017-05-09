package ro.tucn.generator.sender;

import static ro.tucn.util.Topics.K_MEANS;

/**
 * Created by Liviu on 5/9/2017.
 */
public class KMeansSender extends AbstractMessageSender {

    private double[] points;

    @Override
    public void send(Object o) {
        points = (double []) o;
        String messageValue = getMessageValue();
        String messageKey = getMessageKey();
        send(K_MEANS, messageKey, messageValue.toString());
    }

    @Override
    protected String getMessageKey() {
        return null;
    }

    @Override
    protected String getMessageValue() {
        int dimension = points.length;
        StringBuilder messageData = new StringBuilder();
        for (int i = 0; i < dimension - 1; i++) {
            messageData.append(points[i]);
            messageData.append(" ");
        }
        messageData.append(points[dimension - 1]);
        return messageData.toString();
    }
}
