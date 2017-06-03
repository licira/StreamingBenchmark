package ro.tucn.generator.sender;

import ro.tucn.generator.entity.Click;
import ro.tucn.util.Message;

/**
 * Created by Liviu on 5/9/2017.
 */
public class ClickSender extends AbstractMessageSender {

    @Override
    public void send(Object o) {
        Click click = (Click) o;
        String key = getMessageKey(click);
        String value = getMessageValue(click);
        Message message = new Message(key, value);
        String json = toJson(message);
        send(topic, null, json);
    }

    private String getMessageKey(Click click) {
        return click.getAdvId();
    }

    private String getMessageValue(Click click) {
        return String.valueOf(click.getTimestamp());
    }
}
