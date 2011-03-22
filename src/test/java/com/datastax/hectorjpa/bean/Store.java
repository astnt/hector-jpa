/**
 * 
 */
package com.datastax.hectorjpa.bean;

import java.util.HashSet;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
import javax.persistence.Table;

import org.apache.openjpa.persistence.Persistent;

/**
 * @author Todd Nine
 */
@Entity
@Table(name="StoreColumnFamily")
public class Store extends AbstractEntity {

  @Persistent
  private String name;
  
  @OneToMany(mappedBy="store", cascade=CascadeType.ALL)
  @OrderBy("name")
  private Set<Customer> customers;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Set<Customer> getCustomers() {
    return customers;
  }
  
  public void addCustomer(Customer customer){
    if(customers == null){
      customers = new HashSet<Customer>();
    }
    
    customers.add(customer);
  }
}
