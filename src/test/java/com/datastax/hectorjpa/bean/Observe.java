/**
 * 
 */
package com.datastax.hectorjpa.bean;


import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.ManyToOne;

import org.apache.openjpa.persistence.Persistent;

import com.datastax.hectorjpa.annotation.ColumnFamily;

/**
 * Represents the relationship between two users. Essentially a link on the one
 * side of a bi-directional one-to-many with a state. This is a directed edge on
 * a graph. The edge is defined as
 * 
 * follower ---> following
 * 
 * I.E. if Ed follows Nate, Ed's Id is follower and Nate's Id is following. The
 * state of the Following relationship is controlled by the followingId user
 * 
 * @author Todd Nine
 * 
 */
@ColumnFamily("ObserveColumnFamily")
@Entity
public class Observe extends AbstractEntity{


  /**
   * the user observing the target
   */

  @ManyToOne(fetch = FetchType.LAZY, cascade={CascadeType.DETACH, CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH})
  private User owner;

  // don't cascade delete
  @ManyToOne(fetch = FetchType.LAZY, cascade={CascadeType.DETACH, CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH})
  private User target;

  /**
   * TODO TN, these are redundant, but OrderBy won't support non-embedded nested
   * properties. See line 1020 of MetaDataRepsitory from version 2.1.0
   */
  @Persistent
  private String targetFirstName;

  @Persistent
  private String targetLastName;

  @Persistent
  private String ownerFirstName;

  @Persistent
  private String ownerLastName;

  @Persistent
  @Enumerated(EnumType.STRING)
  private FollowState state;

 

  /**
   * @return the follower
   */
  public User getOwner() {
    return owner;
  }

  /**
   * @param follower
   *          the follower to set
   */
  public void setOwner(User follower) {
    this.owner = follower;

    if (follower == null) {
      this.ownerFirstName = null;
      this.ownerLastName = null;
      return;
    }

    this.ownerFirstName = follower.getFirstName();
    this.ownerLastName = follower.getLastName();
  }

  /**
   * @return the following
   */
  public User getTarget() {
    return target;
  }

  /**
   * @param following
   *          the following to set
   */
  public void setTarget(User following) {
    this.target = following;

    if (following == null) {
      targetFirstName = null;
      targetLastName = null;

      return;
    }

    targetFirstName = following.getFirstName();
    targetLastName = following.getLastName();
  }

  /**
   * @return the state
   */
  public FollowState getState() {
    return state;
  }

  /**
   * @param state
   *          the state to set
   */
  public void setState(FollowState state) {
    this.state = state;
  }

  /**
   * @return the followingFirstName
   */
  public String getTargetFirstName() {
    return targetFirstName;
  }

  /**
   * @return the followingLastName
   */
  public String getTargetLastName() {
    return targetLastName;
  }

  /**
   * @return the followerFirstName
   */
  public String getOwnerFirstName() {
    return ownerFirstName;
  }

  /**
   * @return the followerLastName
   */
  public String getOwnerLastName() {
    return ownerLastName;
  }


}
