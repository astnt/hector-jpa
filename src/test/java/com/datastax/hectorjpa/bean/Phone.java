/**
 * 
 */
package com.datastax.hectorjpa.bean;

import java.io.Serializable;

import javax.persistence.Embeddable;
import javax.persistence.Id;

/**
 * Phone to test embedded saving.  All annotations in this object are functionally useless.  However
 * the JPA spec requires an @Embeddable annotation and an @Id annotation.  These are ignored and only 
 * required for runtime enhancement.
 * @author Todd Nine
 *
 */
@Embeddable
public class Phone implements Serializable{
  
  /**
   * DON'T CHANGE ME!
   */
  private static final long serialVersionUID = -6553906640946010478L;


  @Id
  private String phoneNumber;
  
  
  private PhoneType type;
  
  
  public Phone(){
    
  }

  /**
   * 
   * @param phoneNumber
   * @param type
   */
  public Phone(String phoneNumber, PhoneType type){
    this.phoneNumber = phoneNumber;
    this.type = type;
  }
  
  /**
   * @return the phoneNumber
   */
  public String getPhoneNumber() {
    return phoneNumber;
  }




  /**
   * @param phoneNumber the phoneNumber to set
   */
  public void setPhoneNumber(String phoneNumber) {
    this.phoneNumber = phoneNumber;
  }




  /**
   * @return the type
   */
  public PhoneType getType() {
    return type;
  }




  /**
   * @param type the type to set
   */
  public void setType(PhoneType type) {
    this.type = type;
  }




  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((phoneNumber == null) ? 0 : phoneNumber.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }




  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof Phone))
      return false;
    Phone other = (Phone) obj;
    if (phoneNumber == null) {
      if (other.phoneNumber != null)
        return false;
    } else if (!phoneNumber.equals(other.phoneNumber))
      return false;
    if (type != other.type)
      return false;
    return true;
  }




  public enum PhoneType{
    MOBILE,
    HOME
  }

}
