package ro.tucn.generator.sender;

import ro.tucn.generator.entity.Sentence;
import ro.tucn.generator.helper.entity.SentenceJSONHelper;
import ro.tucn.util.Message;

/**
 * Created by liviu.cira on 11.05.2017.
 */
public class SentenceSender extends AbstractMessageSender {

    private SentenceJSONHelper jsonHelper;

    public SentenceSender() {
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
