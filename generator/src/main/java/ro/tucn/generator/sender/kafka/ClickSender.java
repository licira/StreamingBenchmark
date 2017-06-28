package ro.tucn.generator.sender.kafka;

import ro.tucn.generator.entity.Click;
import ro.tucn.generator.helper.entity.ClickJSONHelper;
import ro.tucn.util.Message;

/**
 * Created by Liviu on 5/9/2017.
 */
public class ClickSender extends AbstractKafkaSender {

    private ClickJSONHelper jsonHelper;

    public ClickSender() {
        super();
        jsonHelper = new ClickJSONHelper();
    }

    @Override
    public void send(Object o) {
        Click click = (Click) o;
        String key = jsonHelper.getMessageKey(click);
        String value = jsonHelper.getMessageValue(click);
        Message message = new Message(key, value);
        String json = jsonHelper.toJson(message);
        send(topic, null, json);
    }
}
