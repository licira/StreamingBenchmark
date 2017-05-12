package ro.tucn.generator.sender;

import ro.tucn.generator.entity.Sentence;

import static ro.tucn.util.Topics.SKEWED_WORDS;

/**
 * Created by liviu.cira on 11.05.2017.
 */
public class SentenceSender extends AbstractMessageSender {

	private Sentence sentence;

	@Override
	public void send(Object o) {
		sentence = (Sentence) o;
		String messageValue = getMessageValue();
		String messageKey = getMessageKey();
		send(SKEWED_WORDS, messageKey, messageValue);
	}

	@Override
	protected String getMessageKey() {
		return Integer.toString(sentence.getId());
	}

	@Override
	protected String getMessageValue() {
		int[] words = sentence.getWords();
		int sentenceSize = words.length;
		StringBuilder messageData = new StringBuilder();
		for (int i = 0; i < sentenceSize; i++) {
			messageData.append(Integer.toString(words[i]));
			messageData.append(" ");
		}
		return messageData.toString();
	}
}
