/**
 * 
 */
package com.datastax.hectorjpa.bean.tree;

import java.util.Date;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

import org.apache.openjpa.persistence.Persistent;

import com.datastax.hectorjpa.annotation.ColumnFamily;
import com.datastax.hectorjpa.annotation.Index;
import com.datastax.hectorjpa.annotation.Indexes;
import com.datastax.hectorjpa.bean.AbstractEntity;
import com.eaio.uuid.UUID;

/**
 * @author Matthew Goodson
 *
 */
@Entity
@ColumnFamily("NotificationColumnFamily")
@DiscriminatorValue("Notifiation")
@Index(fields="userId", order="read, createdTime desc")
public class Notification extends AbstractEntity {

	@Persistent
	private UUID userId;
	
	@Persistent
	private Boolean read = false;
	
	@Persistent
	private Date createdTime;
	
	@Persistent
	private String message;
	
	public Notification(UUID userId, String message) {
		this.userId = userId;
		this.message = message;
		setCreatedTime(new Date());
	}

	public UUID getUserId() {
		return userId;
	}

	public void setUserId(UUID userId) {
		this.userId = userId;
	}

	public boolean isRead() {
		return read;
	}

	public void setRead(boolean read) {
		this.read = read;
	}

	public Date getCreatedTime() {
		return createdTime;
	}

	public void setCreatedTime(Date createdTime) {
		this.createdTime = createdTime;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
	
	
	
	
	
	
}
