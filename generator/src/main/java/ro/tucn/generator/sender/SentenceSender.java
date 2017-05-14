package ro.tucn.generator.sender;

import ro.tucn.generator.entity.Sentence;
import ro.tucn.util.Message;

import static ro.tucn.util.Topics.SKEWED_WORDS;

/**
 * Created by liviu.cira on 11.05.2017.
 */
public class SentenceSender extends AbstractMessageSender {

    @Override
    public void send(Object o) {
        Sentence sentence = (Sentence) o;
        String key = getMessageKey(sentence);
        String value = getMessageValue(sentence);
        Message message = new Message(key, value);
        String json = toJson(message);
        send(SKEWED_WORDS, null, json);
    }

    private String getMessageKey(Sentence sentence) {
        return Integer.toString(sentence.getId());
    }

    private String getMessageValue(Sentence sentence) {
        int[] words = sentence.getWords();
        int sentenceSize = words.length;
        StringBuilder messageData = new StringBuilder();
        int i;
        for (i = 0; i < sentenceSize - 1; i++) {
            messageData.append(Integer.toString(words[i]));
            messageData.append(" ");
        }
        messageData.append(Integer.toString(words[i]));
        return messageData.toString();
    }
}
