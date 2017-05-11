package ro.tucn.generator.helper;

import org.apache.commons.math3.random.RandomDataGenerator;
import ro.tucn.generator.entity.Sentence;
import ro.tucn.skewedWords.FastZipfGenerator;

import static ro.tucn.generator.helper.SentenceHelper.SENTENCE_TYPE.SKEWED_WORDS_SENTENCE;
import static ro.tucn.generator.helper.SentenceHelper.SENTENCE_TYPE.UNIFORM_WORDS_SENTENCE;

/**
 * Created by liviu.cira on 11.05.2017.
 */
public class SentenceHelper {

	private RandomDataGenerator messageGenerator;
	private FastZipfGenerator zipfGenerator;

	private double mu;
	private double sigma;
	private int upperBound;
	private int lowerBound;

	public enum SENTENCE_TYPE {
		SKEWED_WORDS_SENTENCE, UNIFORM_WORDS_SENTENCE
	}

	public Sentence createNewSkewedWordsSentence() {
		return getNewSentence(SKEWED_WORDS_SENTENCE);
	}

	public Sentence createNewUniformWordsSentence() {
		return getNewSentence(UNIFORM_WORDS_SENTENCE);
	}

	private Sentence getNewSentence(SENTENCE_TYPE sentenceType) {
		int sentenceLength = (int) messageGenerator.nextGaussian(mu, sigma);
		double[] words = new double[sentenceLength];
		for (int i = 0; i < sentenceLength; ++i) {
			if (sentenceType.equals(SKEWED_WORDS_SENTENCE)) {
				words[i] = zipfGenerator.next();
			} else if (sentenceType.equals(UNIFORM_WORDS_SENTENCE)) {
				words[i] = messageGenerator.nextInt(1, upperBound);
			}
		}
		int sentenceId = messageGenerator.nextInt(1000, 10000);
		Sentence sentence = new Sentence(sentenceId, words);
		return sentence;
	}

	public void setMessageGenerator(RandomDataGenerator messageGenerator) {
		this.messageGenerator = messageGenerator;
	}

	public void setZipfGenerator(FastZipfGenerator zipfGenerator) {
		this.zipfGenerator = zipfGenerator;
	}

	public void setMu(double mu) {
		this.mu = mu;
	}

	public void setSigma(double sigma) {
		this.sigma = sigma;
	}

	public void setUpperBound(int upperBound) {
		this.upperBound = upperBound;
	}
}
