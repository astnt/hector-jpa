package com.datastax.hectorjpa.bean.inheritance;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

/**
 * Warning outbound message
 * 
 * @author Todd Nine
 *
 */
@Entity
@DiscriminatorValue("UberWarningSmsMessage")
public class UberWarningSmsMessage extends WarningSmsMessage {

}
