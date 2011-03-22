/**
 * 
 */
package com.datastax.hectorjpa.bean;

import javax.persistence.Entity;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.apache.openjpa.persistence.Persistent;

/**
 * @author Todd Nine
 *
 */
@Entity
@Table(name="CustomerColumnFamily")
public class Customer extends AbstractEntity {

  @Persistent
  private String name;
  
  @Persistent
  private String phoneNumber;
  
  @Persistent
  private String email;
  
  @ManyToOne
  private Store store;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getPhoneNumber() {
    return phoneNumber;
  }

  public void setPhoneNumber(String phoneNumber) {
    this.phoneNumber = phoneNumber;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public Store getStore() {
    return store;
  }

  public void setStore(Store store) {
    this.store = store;
  }
  
  
}
