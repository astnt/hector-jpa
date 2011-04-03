/**
 * 
 */
package com.datastax.hectorjpa.index;

import java.util.Comparator;

import org.apache.openjpa.meta.Order;

/**
 * Order element or cassandra ordering
 * 
 * @author Todd Nine
 * 
 */
public class FieldOrder implements Order, Comparable<FieldOrder> {

	/**
   * 
   */
	private static final long serialVersionUID = -2234285177676920926L;

	private String fieldName;
	private boolean ascending;

	public FieldOrder(String fieldName, boolean ascending) {
		this.fieldName = fieldName;
		this.ascending = ascending;
	}

	@Override
	public String getName() {
		return fieldName;
	}

	@Override
	public boolean isAscending() {
		return ascending;
	}

	@Override
	public Comparator<?> getComparator() {
		return null;
	}

	@Override
	public int compareTo(FieldOrder o) {
		if (o == null) {
			return 1;
		}

		int compare = this.fieldName.compareTo(o.getName());

		if (compare != 0) {
			return compare;
		}

		if (!ascending && o.isAscending()) {
			return -1;
		} else if (ascending && !o.isAscending()) {
			return 1;
		}

		return 0;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (ascending ? 1231 : 1237);
		result = prime * result
				+ ((fieldName == null) ? 0 : fieldName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof FieldOrder))
			return false;
		FieldOrder other = (FieldOrder) obj;
		if (ascending != other.ascending)
			return false;
		if (fieldName == null) {
			if (other.fieldName != null)
				return false;
		} else if (!fieldName.equals(other.fieldName))
			return false;
		return true;
	}

}
