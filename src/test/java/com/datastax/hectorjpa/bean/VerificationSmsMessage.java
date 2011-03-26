package com.datastax.hectorjpa.bean;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;

/**
 * Verification outbound message
 * 
 * @author Todd Nine
 *
 */
@Entity
@DiscriminatorValue("VerificationSmsMessage")
@Inheritance(strategy=InheritanceType.SINGLE_TABLE)
public class VerificationSmsMessage extends SmsMessage {

}
