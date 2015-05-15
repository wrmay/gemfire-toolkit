package io.pivotal.gemfire_addon.tools;

import java.io.Serializable;
import java.util.Comparator;

import javax.management.ObjectName;

public class ObjectNameComparator implements Comparator<ObjectName>, Serializable {
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(ObjectName left, ObjectName right) {
		if (left == right) return 0; 
		return left.toString().compareTo(right.toString());
	}

}
