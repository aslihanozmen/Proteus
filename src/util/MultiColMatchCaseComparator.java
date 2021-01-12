package util;

import java.util.Comparator;

public class MultiColMatchCaseComparator implements Comparator<MultiColMatchCase> {

	@Override
	public int compare(MultiColMatchCase o1, MultiColMatchCase o2) {
		return o1.getTableID().compareTo(o2.getTableID());
	}

}
