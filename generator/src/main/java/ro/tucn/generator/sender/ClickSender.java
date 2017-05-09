package ro.tucn.generator.sender;

import ro.tucn.generator.entity.Click;

import static ro.tucn.generator.workloadGenerators.AdvClick.CLICK_TOPIC;

/**
 * Created by Liviu on 5/9/2017.
 */
public class ClickSender extends AbstractSender {

    private Click click;

    @Override
    public void send(Object o) {
        click = (Click) o;
        String messageValue = getMessageValue();
        String messageKey = getMessageKey();
        send(CLICK_TOPIC, messageKey, messageValue);
    }

    @Override
    protected String getMessageValue() {
        StringBuilder value = new StringBuilder();
        value.append(click.getTimestamp());
        return value.toString();
    }

    @Override
    protected String getMessageKey() {
        StringBuilder key = new StringBuilder();
        key.append(click.getAdv().getId());
        return key.toString();
    }
}
