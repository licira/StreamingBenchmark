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

    private static int wordsNumberLowerBound = 1;
    private static int wordsNumberUpperBound = 10000;
    private static int wordIdLowerBound = 1000;

    private FastZipfGenerator zipfGenerator;
    private RandomDataGenerator randomDataGenerator;

    private double mu;
    private double sigma;
    private int upperBound;

    public Sentence getNewSkewedWordsSentence() {
        return getNewSentence(SKEWED_WORDS_SENTENCE);
    }

    public Sentence getNewUniformWordsSentence() {
        return getNewSentence(UNIFORM_WORDS_SENTENCE);
    }

    private Sentence getNewSentence(SENTENCE_TYPE sentenceType) {
        int sentenceLength = (int) randomDataGenerator.nextGaussian(mu, sigma);
        int[] words = new int[sentenceLength];
        if (sentenceType.equals(SKEWED_WORDS_SENTENCE)) {
            for (int i = 0; i < sentenceLength; ++i) {
                words[i] = (zipfGenerator.next() + wordsNumberLowerBound) % wordsNumberUpperBound;
            }
        } else if (sentenceType.equals(UNIFORM_WORDS_SENTENCE)) {
            for (int i = 0; i < sentenceLength; ++i) {
                words[i] = randomDataGenerator.nextInt(wordsNumberLowerBound, wordsNumberUpperBound);
            }
        }
        int sentenceId = randomDataGenerator.nextInt(wordIdLowerBound, 10000);
        return new Sentence(sentenceId, words);
    }

    public void setMessageGenerator(RandomDataGenerator messageGenerator) {
        this.randomDataGenerator = messageGenerator;
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

    public void setWordIdLowerBound(int wordIdLowerBound) {
        SentenceHelper.wordIdLowerBound = wordIdLowerBound;
    }

    public void setWordsNumberUpperBound(int wordsNumberUpperBound) {
        SentenceHelper.wordsNumberUpperBound = wordsNumberUpperBound;
    }

    public void setWordsNumberLowerBound(int wordsNumberLowerBound) {
        SentenceHelper.wordsNumberLowerBound = wordsNumberLowerBound;
    }

    public enum SENTENCE_TYPE {
        SKEWED_WORDS_SENTENCE, UNIFORM_WORDS_SENTENCE
    }
}
