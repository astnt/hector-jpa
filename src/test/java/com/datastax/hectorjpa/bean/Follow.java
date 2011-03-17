/**
 * 
 */
package com.datastax.hectorjpa.bean;

import java.io.Serializable;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.IdClass;
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
@IdClass(Follow.FollowId.class)
@Table(name = "FollowColumnFamily")
@Entity
public class Follow {
  
  
  @Persistent(mappedBy="following", cascade={CascadeType.PERSIST, CascadeType.MERGE, CascadeType.REFRESH, CascadeType.DETACH} )
  @Id
  private User follower;
  
  @Persistent(mappedBy="followers", cascade={CascadeType.PERSIST, CascadeType.MERGE, CascadeType.REFRESH, CascadeType.DETACH} )
  @Id
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
    this.followingFirstName = following.getFirstName();
    this.followingLastName = following.getLastName();
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

  public static class FollowId implements Serializable{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    
    private UUID follower;
    
    private UUID following;
    

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((follower == null) ? 0 : follower.hashCode());
      result = prime * result
          + ((following == null) ? 0 : following.hashCode());
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
      if (getClass() != obj.getClass())
        return false;
      FollowId other = (FollowId) obj;
      if (follower == null) {
        if (other.follower != null)
          return false;
      } else if (!follower.equals(other.follower))
        return false;
      if (following == null) {
        if (other.following != null)
          return false;
      } else if (!following.equals(other.following))
        return false;
      return true;
    }
    
    
    
  }
  

}
