package ro.tucn.generator.sender.offline;

import ro.tucn.generator.entity.Sentence;
import ro.tucn.generator.helper.entity.SentenceJSONHelper;
import ro.tucn.util.Message;

/**
 * Created by Liviu on 6/28/2017.
 */
public class SentenceSenderOffline extends AbstractOfflineSender {

    private SentenceJSONHelper jsonHelper;

    public SentenceSenderOffline() {
        super();
        jsonHelper = new SentenceJSONHelper();
    }

    @Override
    public void send(Object o) {
        Sentence sentence = (Sentence) o;
        String key = jsonHelper.getMessageKey(sentence);
        String value = jsonHelper.getMessageValue(sentence);
        Message message = new Message(key, value);
        String json = jsonHelper.toJson(message);
        send(topic, null, json);
    }
}
