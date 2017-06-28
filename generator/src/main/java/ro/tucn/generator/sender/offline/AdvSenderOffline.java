package ro.tucn.generator.sender.offline;

import ro.tucn.generator.entity.Adv;
import ro.tucn.generator.helper.entity.AdvJSONHelper;
import ro.tucn.util.Message;

/**
 * Created by Liviu on 6/28/2017.
 */
public class AdvSenderOffline extends AbstractOfflineSender {

    private AdvJSONHelper jsonHelper;

    public AdvSenderOffline(){
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
