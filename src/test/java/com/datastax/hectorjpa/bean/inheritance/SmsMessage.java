/**
 * 
 */
package com.datastax.hectorjpa.bean.inheritance;

import java.util.Date;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

import org.apache.openjpa.persistence.Persistent;

import com.datastax.hectorjpa.annotation.ColumnFamily;
import com.datastax.hectorjpa.annotation.Index;
import com.datastax.hectorjpa.bean.AbstractEntity;

/**
 * Abstract class common to all sms messages
 * 
 * @author Todd Nine
 *
 */
@Entity
@DiscriminatorValue("SmsMessage")
@ColumnFamily("SmsMessageColumnFamily")
@Index(fields="phoneNumber, messageId", order="createdDate desc")

public abstract class SmsMessage extends AbstractEntity {

	@Persistent
	private String phoneNumber;
	
	@Persistent
	private int messageId;
	
	@Persistent
	private Date createdDate;

	public String getPhoneNumber() {
		return phoneNumber;
	}

	public void setPhoneNumber(String phoneNumber) {
		this.phoneNumber = phoneNumber;
	}

	public int getMessageId() {
		return messageId;
	}

	public void setMessageId(int messageId) {
		this.messageId = messageId;
	}

	public Date getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(Date createdDate) {
		this.createdDate = createdDate;
	}
	
	
	
}
