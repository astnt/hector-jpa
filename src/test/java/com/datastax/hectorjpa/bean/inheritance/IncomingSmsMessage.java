package com.datastax.hectorjpa.bean.inheritance;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;


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
