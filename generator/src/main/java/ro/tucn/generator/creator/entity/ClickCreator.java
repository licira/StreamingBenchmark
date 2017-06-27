package ro.tucn.generator.creator.entity;

import ro.tucn.generator.entity.Adv;
import ro.tucn.generator.entity.Click;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by liviu.cira on 11.05.2017.
 */
public class ClickCreator {

	public Click getNewClick(Adv adv) {
		return new Click(adv.getId());
	}

	public List<Click> getNewClicks(List<Adv> advs) {
		List<Click> clicks = new ArrayList<>();
		for (Adv adv : advs) {
			clicks.add(getNewClick(adv));
		}
		return clicks;
	}
}
