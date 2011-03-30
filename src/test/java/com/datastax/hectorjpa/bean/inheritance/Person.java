/**
 * 
 */
package com.datastax.hectorjpa.bean.inheritance;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

import org.apache.openjpa.persistence.Persistent;

import com.datastax.hectorjpa.annotation.ColumnFamily;
import com.datastax.hectorjpa.annotation.Index;
import com.datastax.hectorjpa.bean.AbstractEntity;

/**
 * 
 * @author Todd Nine
 *
 */
@Entity
@ColumnFamily("PersonColumnFamily")
@DiscriminatorValue("Person")
@Index(fields="email", order="firstName, lastName")
public class Person extends AbstractEntity {


  @Persistent
  private String firstName;

  @Persistent
  private String lastName;

  @Persistent
  private String email;

  /**
   * @return the firstName
   */
  public String getFirstName() {
    return firstName;
  }

  /**
   * @param firstName the firstName to set
   */
  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  /**
   * @return the lastName
   */
  public String getLastName() {
    return lastName;
  }

  /**
   * @param lastName the lastName to set
   */
  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  /**
   * @return the email
   */
  public String getEmail() {
    return email;
  }

  /**
   * @param email the email to set
   */
  public void setEmail(String email) {
    this.email = email;
  }
  
  
}
