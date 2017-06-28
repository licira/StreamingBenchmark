package ro.tucn.generator.sender.offline;

import ro.tucn.generator.entity.Click;
import ro.tucn.generator.helper.entity.ClickJSONHelper;
import ro.tucn.util.Message;

/**
 * Created by Liviu on 6/28/2017.
 */
public class ClickSenderOffline extends AbstractOfflineSender {

    private ClickJSONHelper jsonHelper;

    public ClickSenderOffline() {
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
