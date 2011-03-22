/**
 * 
 */
package com.datastax.hectorjpa.bean;

import java.io.Serializable;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.ManyToOne;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.openjpa.persistence.Persistent;

import com.eaio.uuid.UUID;

/**
 * Represents the relationship between two users.  Essentially a link on the one side of a 
 * bi-directional one-to-many with a state.  This is a directed edge on a graph.  The edge is defined as
 * 
 * follower ---> following
 * 
 * I.E. if Ed follows Nate, Ed's Id is follower and Nate's Id is following.  The state of the Following
 * relationship is controlled by the followingId user
 * 
 * @author Todd Nine
 *
 */
//@IdClass(Follow.FollowId.class)
@Table(name = "FollowColumnFamily")
@Entity
@SequenceGenerator(name = "timeuuid", allocationSize = 100, sequenceName = "com.datastax.hectorjpa.sequence.TimeUuid()")
public class Follow {
  
  @Id
  @Persistent
  @GeneratedValue(generator = "timeuuid", strategy = GenerationType.SEQUENCE)
  private UUID id;

  
  //don't cascade delete
  @ManyToOne(cascade={CascadeType.DETACH, CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH} )
  private User follower;
  
  //don't cascade delete
  @ManyToOne(cascade={CascadeType.DETACH, CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH} )
  private User following;
  
  /**
   * TODO TN, these are redundant, but OrderBy won't support non-embedded nested properties.  See line 1020 of MetaDataRepsitory from version 2.1.0 
   */
  @Persistent
  private String followingFirstName;
  
  @Persistent
  private String followingLastName;
  
  @Persistent
  private String followerFirstName;
  
  @Persistent
  private String followerLastName;
  
  
  @Persistent
  @Enumerated(EnumType.STRING)
  private FollowState state;
  
  
  /**
   * @return the id
   */
  public UUID getId() {
    return id;
  }

  
  
  /**
   * @return the follower
   */
  public User getFollower() {
    return follower;
  }

  /**
   * @param follower the follower to set
   */
  public void setFollower(User follower) {
    this.follower = follower;
    
    if(follower == null){
      this.followerFirstName = null;
      this.followerLastName = null;
      return;
    }
    
    this.followerFirstName = follower.getFirstName();
    this.followerLastName = follower.getLastName();
  }

  /**
   * @return the following
   */
  public User getFollowing() {
    return following;
  }

  /**
   * @param following the following to set
   */
  public void setFollowing(User following) {
    this.following = following;
    
    if(following == null){
      followingFirstName = null;
      followingLastName = null;
      
      return;
    }
    
    followingFirstName = following.getFirstName();
    followingLastName = following.getLastName();
  }

  /**
   * @return the state
   */
  public FollowState getState() {
    return state;
  }

  /**
   * @param state the state to set
   */
  public void setState(FollowState state) {
    this.state = state;
  }
  
  /**
   * @return the followingFirstName
   */
  public String getFollowingFirstName() {
    return followingFirstName;
  }

  /**
   * @return the followingLastName
   */
  public String getFollowingLastName() {
    return followingLastName;
  }

  /**
   * @return the followerFirstName
   */
  public String getFollowerFirstName() {
    return followerFirstName;
  }

  /**
   * @return the followerLastName
   */
  public String getFollowerLastName() {
    return followerLastName;
  }



  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((id == null) ? 0 : id.hashCode());
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
    if (!(obj instanceof Follow))
      return false;
    Follow other = (Follow) obj;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    return true;
  }
  
  

//  public static class FollowId implements Serializable{
//
//    /**
//     * 
//     */
//    private static final long serialVersionUID = 1L;
//    
//    
//    private UUID follower;
//    
//    private UUID following;
//    
//
//    /* (non-Javadoc)
//     * @see java.lang.Object#hashCode()
//     */
//    @Override
//    public int hashCode() {
//      final int prime = 31;
//      int result = 1;
//      result = prime * result + ((follower == null) ? 0 : follower.hashCode());
//      result = prime * result
//          + ((following == null) ? 0 : following.hashCode());
//      return result;
//    }
//
//    /* (non-Javadoc)
//     * @see java.lang.Object#equals(java.lang.Object)
//     */
//    @Override
//    public boolean equals(Object obj) {
//      if (this == obj)
//        return true;
//      if (obj == null)
//        return false;
//      if (getClass() != obj.getClass())
//        return false;
//      FollowId other = (FollowId) obj;
//      if (follower == null) {
//        if (other.follower != null)
//          return false;
//      } else if (!follower.equals(other.follower))
//        return false;
//      if (following == null) {
//        if (other.following != null)
//          return false;
//      } else if (!following.equals(other.following))
//        return false;
//      return true;
//    }
//    
//    
//    
//  }
  

}
