package com.datastax.hectorjpa.bean.inheritance;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;

/**
 * Warning outbound message
 * 
 * @author Todd Nine
 *
 */
@Entity
@DiscriminatorValue("WarningSmsMessage")
@Inheritance(strategy=InheritanceType.SINGLE_TABLE)
public class WarningSmsMessage extends SmsMessage {

}
