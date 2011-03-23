/**
 * 
 */
package com.datastax.hectorjpa.bean;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
import javax.persistence.Table;

import org.apache.openjpa.persistence.FetchAttribute;
import org.apache.openjpa.persistence.Persistent;
import org.apache.openjpa.persistence.jdbc.Index;

/**
 * Generic user class with auto generated entity
 * 
 * @author Todd Nine
 * 
 */
@Entity
@Table(name = "UserColumnFamily")
public class User extends AbstractEntity {


  @Persistent
  private String firstName;

  @Persistent
  private String lastName;

  @Persistent
  @Index
  private String email;

  /**
   * People who are following me (I.E graph edge into user's node)
   */
  @OneToMany(mappedBy = "target", cascade = CascadeType.ALL, orphanRemoval=true, fetch=FetchType.LAZY)
  @OrderBy("ownerFirstName, ownerLastName")
  private List<Observe> observers;

  /**
   * People who I'm following (I.E graph edge out from user's node)
   */
  @OneToMany(mappedBy = "owner", cascade = CascadeType.ALL, orphanRemoval=true, fetch=FetchType.LAZY)
  @OrderBy("targetFirstName, targetLastName")
  private List<Observe> observing;

  

  // no set id intentionally, we want the framework to set it the first time
  // this entity is saved

  /**
   * @return the firstName
   */
  public String getFirstName() {
    return firstName;
  }

  /**
   * @param firstName
   *          the firstName to set
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
   * @param lastName
   *          the lastName to set
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
   * @param email
   *          the email to set
   */
  public void setEmail(String email) {
    this.email = email;
  }

  /**
   * Null safe get, will always return an empty list if no elements are present
   * 
   * @return Users who are observing me
   */
  public List<Observe> getObservers() {
    if (observers == null) {
      //we use hash sets, this will get wrapped with a proxy and ordered after first save, the the first impl 
      //we use is irrelevant since the set will really be a proxy after first save
      observers = new ArrayList<Observe>();
    }

    return observers;
  }

  /**
   * Null safe get, will always return an empty list if no elements are present
   * 
   * @return Users I am observing
   * 
   */
  public List<Observe> getObserving() {
    if (observing == null) {
      //we use hash sets, this will get wrapped with a proxy and ordered after first save, the the first impl 
      //we use is irrelevant since the set will really be a proxy after first save
      observing = new ArrayList<Observe>();
    }

    return observing;
  }

  /**
   * Follow the target user with the given state
   * 
   * @param target
   * @param state
   */
  public void observeUser(User target, FollowState state) {

    // add the out bound edge to the following
    Observe observe = new Observe();
 
    //link to the target
    observe.setTarget(target);
    observe.setOwner(this);
    observe.setState(state);
    
    target.getObservers().add(observe);        
    // we're following
    getObserving().add(observe);
    
    
  }


}
