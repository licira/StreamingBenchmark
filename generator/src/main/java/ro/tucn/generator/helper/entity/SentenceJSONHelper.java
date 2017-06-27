package ro.tucn.generator.helper.entity;

import ro.tucn.generator.entity.Sentence;

/**
 * Created by Liviu on 6/27/2017.
 */
public class SentenceJSONHelper extends JSONHelper {

    public String getMessageKey(Sentence sentence) {
        return Integer.toString(sentence.getId());
    }

    public String getMessageValue(Sentence sentence) {
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
