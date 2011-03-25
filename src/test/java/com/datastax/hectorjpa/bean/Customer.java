/**
 * 
 */
package com.datastax.hectorjpa.bean;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
import javax.persistence.Table;

import org.apache.openjpa.persistence.Persistent;

/**
 * @author Todd Nine
 * 
 */
@Entity
@Table(name = "CustomerColumnFamily")
public class Customer extends AbstractEntity {

  @Persistent
  private String name;

  @Persistent
  private String phoneNumber;

  @Persistent
  private String email;

  @ManyToOne
  private Store store;

  @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true, mappedBy = "customer")
  private List<Sale> sales;

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

  /**
   * @return the sales
   */
  public List<Sale> getSales() {

    return sales;
  }

  /**
   * Add the sale to the customer
   * @param sale
   */
  public void addSale(Sale sale) {
    if (sales == null) {
      this.sales = new ArrayList<Sale>();
    }

    this.sales.add(sale);
  }

}
