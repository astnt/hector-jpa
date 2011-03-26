/**
 * 
 */
package com.datastax.hectorjpa.bean;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.ManyToOne;

import org.apache.openjpa.persistence.Persistent;

import com.datastax.hectorjpa.annotation.ColumnFamily;
import com.datastax.hectorjpa.annotation.Index;

/**
 * A class to simulate customer purchases.
 * @author Todd Nine
 *
 */
@Entity
@ColumnFamily("SaleColumnFamily")
public class Sale extends AbstractEntity {

  @Persistent
  //Search on item name, sort by sellDate
  @Index("sellDate")
  private String itemName;
  
  
  @Persistent
  //search on sellDate order by item name
  @Index("itemName")
  private Date sellDate;

  @ManyToOne
  private Customer customer;
  
  /**
   * @return the itemName
   */
  public String getItemName() {
    return itemName;
  }

  /**
   * @param itemName the itemName to set
   */
  public void setItemName(String itemName) {
    this.itemName = itemName;
  }

  /**
   * @return the purchasedDate
   */
  public Date getSellDate() {
    return sellDate;
  }

  /**
   * @param purchasedDate the purchasedDate to set
   */
  public void setSellDate(Date purchasedDate) {
    this.sellDate = purchasedDate;
  }

  /**
   * @return the customer
   */
  public Customer getCustomer() {
    return customer;
  }

  /**
   * @param customer the customer to set
   */
  public void setCustomer(Customer customer) {
    this.customer = customer;
  }
  
  
  
}
