/**
 * 
 */
package com.datastax.hectorjpa.bean;

import java.io.Serializable;

import javax.persistence.Embeddable;

import org.apache.openjpa.persistence.Persistent;
import org.apache.openjpa.persistence.jdbc.Strategy;

import com.eaio.uuid.UUID;

/**
 * Phone to test embedded saving. All annotations in this object are
 * functionally useless. However the JPA spec requires an @Embeddable annotation
 * and an @Id annotation. These are ignored and only required for runtime
 * enhancement.
 * 
 * @author Todd Nine
 * 
 */
@Embeddable
public class Phone implements Serializable {

	/**
	 * DON'T CHANGE ME!
	 */
	private static final long serialVersionUID = -6553906640946010478L;

	@Persistent
	@Strategy("com.datastax.hectorjpa.strategy.NoOpHandler")
	private UUID id;

	@Persistent
	private String phoneNumber;

	@Persistent
	private PhoneType type;

	public Phone() {
		this.id = new UUID();
	}

	/**
	 * 
	 * @param phoneNumber
	 * @param type
	 */
	public Phone(String phoneNumber, PhoneType type) {
		this();
		this.phoneNumber = phoneNumber;
		this.type = type;
	}

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	/**
	 * @return the phoneNumber
	 */
	public String getPhoneNumber() {
		return phoneNumber;
	}

	/**
	 * @param phoneNumber
	 *            the phoneNumber to set
	 */
	public void setPhoneNumber(String phoneNumber) {
		this.phoneNumber = phoneNumber;
	}

	/**
	 * @return the type
	 */
	public PhoneType getType() {
		return type;
	}

	/**
	 * @param type
	 *            the type to set
	 */
	public void setType(PhoneType type) {
		this.type = type;
	}



	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Phone))
			return false;
		Phone other = (Phone) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}



	public enum PhoneType {
		MOBILE, HOME
	}

}
