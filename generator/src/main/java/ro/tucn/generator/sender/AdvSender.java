package ro.tucn.generator.sender;

import ro.tucn.generator.entity.Adv;

import static ro.tucn.generator.workloadGenerators.AdvClickGenerator.ADV_TOPIC;

/**
 * Created by Liviu on 5/9/2017.
 */
public class AdvSender extends AbstractMessageSender {

    private Adv adv;

    @Override
    public void send(Object o) {
        adv = (Adv) o;
        String messageValue = getMessageValue();
        String messageKey = getMessageKey();
        send(ADV_TOPIC, messageKey, messageValue);
    }

    @Override
    protected String getMessageValue() {
        StringBuilder value = new StringBuilder();
        value.append(adv.getTimestamp());
        return value.toString();
    }

    @Override
    protected String getMessageKey() {
        StringBuilder key = new StringBuilder();
        key.append(adv.getId());
        return key.toString();
    }
}
