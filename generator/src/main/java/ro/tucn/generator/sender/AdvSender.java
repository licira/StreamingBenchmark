package ro.tucn.generator.sender;

import ro.tucn.generator.entity.Adv;
import ro.tucn.util.Message;

/**
 * Created by Liviu on 5/9/2017.
 */
public class AdvSender extends AbstractMessageSender {

    @Override
    public void send(Object o) {
        Adv adv = (Adv) o;
        String key = getMessageKey(adv);
        String value = getMessageValue(adv);
        Message message = new Message(key, value);
        String json = toJson(message);
        send(topic, null, json);
    }

    private String getMessageKey(Adv adv) {
        return adv.getId();
    }

    private String getMessageValue(Adv adv) {
        return String.valueOf(adv.getTimestamp());
    }
}
