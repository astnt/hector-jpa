/**
 * 
 */
package com.datastax.hectorjpa.bean.inheritance;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;

import org.apache.openjpa.persistence.Persistent;

import com.datastax.hectorjpa.annotation.Index;
import com.datastax.hectorjpa.annotation.Indexes;

/**
 * @author Todd Nine
 *
 */
@Entity
@Inheritance(strategy=InheritanceType.SINGLE_TABLE)
@DiscriminatorValue("Client")
@Indexes({
@Index(fields="lastLogin", order="firstName, lastName"),
@Index(fields="numberOfVisits", order="lastLogin desc")
})
public class Client extends User {

  @Persistent
  private int numberOfVisits;

  /**
   * @return the numberOfPurchases
   */
  public int getNumberOfVisits() {
    return numberOfVisits;
  }

  /**
   * @param numberOfPurchases the numberOfPurchases to set
   */
  public void setNumberOfVisits(int numberOfPurchases) {
    this.numberOfVisits = numberOfPurchases;
  }
  
  
}
