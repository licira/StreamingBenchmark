package ro.tucn.generator.entity;

/**
 * Created by Liviu on 5/7/2017.
 */
public class Click {

    private String advId;
    private long timestamp;

    public Click(String advId) {
        this.advId = advId;
        timestamp = System.nanoTime();
    }

    public String getAdvId() {
        return this.advId;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
