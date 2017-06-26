package ro.tucn.generator.creator;

import ro.tucn.generator.entity.Adv;
import ro.tucn.generator.entity.Click;

/**
 * Created by liviu.cira on 11.05.2017.
 */
public class ClickCreator {

	public static Click createNewClick(Adv adv) {
		return new Click(adv.getId());
	}
}
