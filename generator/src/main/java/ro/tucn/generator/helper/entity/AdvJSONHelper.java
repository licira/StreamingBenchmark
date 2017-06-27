package ro.tucn.generator.helper.entity;

import ro.tucn.generator.entity.Adv;

/**
 * Created by Liviu on 6/27/2017.
 */
public class AdvJSONHelper extends JSONHelper {

    public String getMessageKey(Adv adv) {
        return adv.getId();
    }

    public String getMessageValue(Adv adv) {
        return String.valueOf(adv.getTimestamp());
    }
}
