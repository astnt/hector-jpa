/**
 * 
 */
package com.datastax.hectorjpa.bean;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.apache.openjpa.persistence.Persistent;

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
// @IdClass(Follow.FollowId.class)
@Table(name = "ObserveColumnFamily")
@Entity
public class Observe extends AbstractEntity{


  /**
   * the user observing the target
   */
  @ManyToOne(fetch=FetchType.LAZY)
  private User owner;

  // don't cascade delete
  @ManyToOne(fetch=FetchType.LAZY)
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


  // public static class FollowId implements Serializable{
  //
  // /**
  // *
  // */
  // private static final long serialVersionUID = 1L;
  //
  //
  // private UUID follower;
  //
  // private UUID following;
  //
  //
  // /* (non-Javadoc)
  // * @see java.lang.Object#hashCode()
  // */
  // @Override
  // public int hashCode() {
  // final int prime = 31;
  // int result = 1;
  // result = prime * result + ((follower == null) ? 0 : follower.hashCode());
  // result = prime * result
  // + ((following == null) ? 0 : following.hashCode());
  // return result;
  // }
  //
  // /* (non-Javadoc)
  // * @see java.lang.Object#equals(java.lang.Object)
  // */
  // @Override
  // public boolean equals(Object obj) {
  // if (this == obj)
  // return true;
  // if (obj == null)
  // return false;
  // if (getClass() != obj.getClass())
  // return false;
  // FollowId other = (FollowId) obj;
  // if (follower == null) {
  // if (other.follower != null)
  // return false;
  // } else if (!follower.equals(other.follower))
  // return false;
  // if (following == null) {
  // if (other.following != null)
  // return false;
  // } else if (!following.equals(other.following))
  // return false;
  // return true;
  // }
  //
  //
  //
  // }

}
