package ro.tucn.generator.entity;

/**
 * Created by Liviu on 5/7/2017.
 */
public class Click {

    private Adv adv;
    private long timestamp;

    public Click(Adv adv) {
        this.adv = adv;
        timestamp = System.nanoTime();
    }

    public Adv getAdv() {
        return adv;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
