package ro.tucn.generator.helper;

import ro.tucn.generator.entity.Adv;

import java.util.UUID;

/**
 * Created by Liviu on 5/7/2017.
 */
public class AdvHelper {

    public static Adv createNewAdv() {
        String advId = UUID.randomUUID().toString();
        long timestamp = TimeHelper.getNanoTime();
        Adv adv = new Adv(advId, timestamp);
        return adv;
    }
}
