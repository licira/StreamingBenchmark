package ro.tucn.generator.entity;

/**
 * Created by liviu.cira on 11.05.2017.
 */
public class Sentence {

	private int id;
	private double[] words;

	public Sentence(int id, double[] words) {
		this.id = id;
		this.words = words;
	}

	public double[] getWords() {
		return words;
	}

	public void setWords(double[] words) {
		this.words = words;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
}
