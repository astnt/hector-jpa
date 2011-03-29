package com.datastax.hectorjpa.bean;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;

import com.datastax.hectorjpa.bean.inheritance.SmsMessage;

/**
 * Incoming sms messages
 * 
 * @author Todd Nine
 *
 */
@Entity
@DiscriminatorValue("IncomingSmsMessage")
@Inheritance(strategy=InheritanceType.SINGLE_TABLE)
public class IncomingSmsMessage extends SmsMessage {

}
