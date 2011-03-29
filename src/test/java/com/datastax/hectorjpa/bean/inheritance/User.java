/**
 * 
 */
package com.datastax.hectorjpa.bean.inheritance;

import java.util.Date;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;

import org.apache.openjpa.persistence.Persistent;

import com.datastax.hectorjpa.annotation.Index;

/**
 * @author Todd Nine
 *
 */
@Entity
@Inheritance(strategy=InheritanceType.SINGLE_TABLE)
@DiscriminatorValue("User")
@Index(fields="lastLogin", order="firstName, lastName")
public class User extends Person {

  @Persistent
  private Date lastLogin;

  /**
   * @return the lastLogin
   */
  public Date getLastLogin() {
    return lastLogin;
  }

  /**
   * @param lastLogin the lastLogin to set
   */
  public void setLastLogin(Date lastLogin) {
    this.lastLogin = lastLogin;
  }
  
  
}
