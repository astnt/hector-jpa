/**
 * 
 */
package com.datastax.hectorjpa.bean;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.MappedSuperclass;

import org.apache.openjpa.persistence.Persistent;

import com.datastax.hectorjpa.annotation.ColumnFamily;

/**
 * Abstract class common to all sms messages
 * 
 * @author Todd Nine
 *
 */
@MappedSuperclass
@ColumnFamily("SmsMessageColumnFamily")
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
