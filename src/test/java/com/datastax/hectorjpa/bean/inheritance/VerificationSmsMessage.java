package com.datastax.hectorjpa.bean.inheritance;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

/**
 * Verification outbound message
 * 
 * @author Todd Nine
 *
 */
@Entity
@DiscriminatorValue("VerificationSmsMessage")
public class VerificationSmsMessage extends SmsMessage {

}
