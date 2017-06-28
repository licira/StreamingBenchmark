package ro.tucn.generator.sender.kafka;

import ro.tucn.generator.entity.Adv;
import ro.tucn.generator.helper.entity.AdvJSONHelper;
import ro.tucn.util.Message;

/**
 * Created by Liviu on 5/9/2017.
 */
public class AdvSenderKafka extends AbstractKafkaSender {

    private AdvJSONHelper jsonHelper;

    public AdvSenderKafka() {
        super();
        jsonHelper = new AdvJSONHelper();
    }

    @Override
    public void send(Object o) {
        Adv adv = (Adv) o;
        String key = jsonHelper.getMessageKey(adv);
        String value = jsonHelper.getMessageValue(adv);
        Message message = new Message(key, value);
        String json = jsonHelper.toJson(message);
        send(topic, null, json);
    }
}