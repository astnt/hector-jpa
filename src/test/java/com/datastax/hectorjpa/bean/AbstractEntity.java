/**
 * 
 */
package com.datastax.hectorjpa.bean;

import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.SequenceGenerator;

import org.apache.openjpa.persistence.Persistent;

import com.eaio.uuid.UUID;

/**
 * @author Todd Nine
 *
 */
@SequenceGenerator(name = "timeuuid", allocationSize = 100, sequenceName = "com.datastax.hectorjpa.sequence.TimeUuid()")
@MappedSuperclass
public class AbstractEntity {

  @Id
  @Persistent
  @GeneratedValue(generator = "timeuuid", strategy = GenerationType.SEQUENCE)
  private UUID id;

  /**
   * Get the Id
   * @return
   */
  public UUID getId() {
    return id;
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
    if (!(obj instanceof AbstractEntity))
      return false;
    AbstractEntity other = (AbstractEntity) obj;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    return true;
  }
}
