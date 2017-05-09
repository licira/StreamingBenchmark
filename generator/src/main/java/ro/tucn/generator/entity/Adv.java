package ro.tucn.generator.entity;

/**
 * Created by Liviu on 5/7/2017.
 */
public class Adv implements Comparable<Adv> {

    private String id;
    private long timestamp;

    public Adv(String id, long timestamp) {
        this.id = id;
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(Adv o) {
        if (this.timestamp > o.getTimestamp())
            return 1;
        else if (this.timestamp == o.getTimestamp())
            return 0;
        else
            return -1;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long time) {
        this.timestamp = time;
    }
}
