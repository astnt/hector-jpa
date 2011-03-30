/**
 * 
 */
package com.datastax.hectorjpa.bean.inheritance;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

import org.apache.openjpa.persistence.Persistent;

import com.datastax.hectorjpa.annotation.Index;

/**
 * @author Todd Nine
 *
 */
@Entity
@DiscriminatorValue("Manager")
@Index(fields="numberOfClients", order="lastLogin desc")
public class Manager extends User {

  @Persistent
  private int numberOfClients;

  /**
   * @return the numberOfPurchases
   */
  public int getNumberOfClients() {
    return numberOfClients;
  }

  /**
   * @param numberOfPurchases the numberOfPurchases to set
   */
  public void setNumberOfClients(int numberOfPurchases) {
    this.numberOfClients = numberOfPurchases;
  }
  
  
}
