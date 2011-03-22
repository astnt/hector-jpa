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
}
