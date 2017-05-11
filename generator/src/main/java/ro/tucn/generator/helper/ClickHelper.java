package ro.tucn.generator.helper;

import ro.tucn.generator.entity.Adv;
import ro.tucn.generator.entity.Click;

/**
 * Created by liviu.cira on 11.05.2017.
 */
public class ClickHelper {

	public static Click createNewClick(Adv adv) {
		return new Click(adv);
	}
}
