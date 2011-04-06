/**
 * 
 */
package com.datastax.hectorjpa.bean;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;

import org.apache.openjpa.persistence.Persistent;

import com.datastax.hectorjpa.annotation.ColumnFamily;
import com.datastax.hectorjpa.annotation.Index;

/**
 * @author Todd Nine
 */
@Entity
@ColumnFamily("StoreColumnFamily")
//create an index used only for iteration on querying
@Index(fields="name")
public class Store extends AbstractEntity {

  @Persistent
  private String name;
  
  @OneToMany(mappedBy="config", orphanRemoval=true, cascade=CascadeType.ALL)
  @OrderBy("name")
  private List<Customer> customers;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<Customer> getCustomers() {
    return customers;
  }
  
  public void addCustomer(Customer customer){
    if(customers == null){
      customers = new ArrayList<Customer>();
    }
    
    customers.add(customer);
  }
}
