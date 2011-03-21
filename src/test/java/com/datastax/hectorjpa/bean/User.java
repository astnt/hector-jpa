/**
 * 
 */
package com.datastax.hectorjpa.bean;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import org.apache.openjpa.persistence.Persistent;
import org.apache.openjpa.persistence.jdbc.Index;

import com.eaio.uuid.UUID;

/**
 * Generic user class with auto generated entity
 * 
 * @author Todd Nine
 * 
 */
@Entity
@Table(name = "UserColumnFamily")
@SequenceGenerator(name = "timeuuid", allocationSize = 100, sequenceName = "com.datastax.hectorjpa.sequence.TimeUuid()")
public class User {

  @Id
  @Persistent
  @GeneratedValue(generator = "timeuuid", strategy = GenerationType.SEQUENCE)
  private UUID id;

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
  @OneToMany(mappedBy = "following", cascade = CascadeType.ALL)
  @OrderBy("followerFirstName, followerLastName")
  private List<Follow> followers;

  /**
   * People who I'm following (I.E graph edge out from user's node)
   */
  @OneToMany(mappedBy = "followers", cascade = CascadeType.ALL)
  @OrderBy("followingFirstName, followingLastName")
  private List<Follow> following;

  /**
   * @return the id
   */
  public UUID getId() {
    return id;
  }

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
   * @return the followers
   */
  public List<Follow> getFollowers() {
    if (followers == null) {
      followers = new ArrayList<Follow>();
    }

    return followers;
  }

  /**
   * Null safe get, will always return an empty list if no elements are present
   * 
   * @return the following
   */
  public List<Follow> getFollowing() {
    if (following == null) {
      following = new ArrayList<Follow>();
    }

    return following;
  }

  /**
   * Follow the target user with the given state
   * 
   * @param target
   * @param state
   */
  public void followUser(User target, FollowState state) {

    // add the out bound edge to the following
    Follow follow = new Follow();
    follow.setFollowing(target);
    follow.setFollower(this);
    follow.setState(state);

    // we're following
    getFollowing().add(follow);

    // // notify the target they're followed
     target.getFollowers().add(follow);
    
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof User))
      return false;
    User other = (User) obj;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    return true;
  }

}
