package ro.tucn.generator.helper.entity;

import ro.tucn.generator.entity.Click;

/**
 * Created by Liviu on 6/27/2017.
 */
public class ClickJSONHelper extends JSONHelper {

    public String getMessageKey(Click click) {
        return click.getAdvId();
    }

    public String getMessageValue(Click click) {
        return String.valueOf(click.getTimestamp());
    }
}
