package ro.tucn.generator.creator.entity;

import ro.tucn.generator.entity.Adv;
import ro.tucn.generator.helper.TimeHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by Liviu on 5/7/2017.
 */
public class AdvCreator {

    public static Adv getNewAdv() {
        String advId = UUID.randomUUID().toString();
        long timestamp = TimeHelper.getNanoTime();
        return new Adv(advId, timestamp);
    }

    public List<Adv> getNewAdvs(long n) {
        List<Adv> advs = new ArrayList<>();
        for (long i = 0; i < n; i++) {
            advs.add(getNewAdv());
        }
        return advs;
    }
}
