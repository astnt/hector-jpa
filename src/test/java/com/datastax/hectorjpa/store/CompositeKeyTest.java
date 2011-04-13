package com.datastax.hectorjpa.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import javax.persistence.EntityManager;

import org.junit.Test;

import com.datastax.hectorjpa.ManagedEntityTestBase;
import com.datastax.hectorjpa.bean.Magazine;

/**
 * 
 * SETUP NOTES: Unfortunately the open JPA plugin lacks the ability to generate
 * meta data to resolve compilation issues run a test compile from the command
 * line, then include "target/generated-sources/test-annotations" as a source
 * directory in eclipse.
 * 
 * Tests saving retrieving via composite key
 * 
 * @author Todd Nine
 * 
 */
public class CompositeKeyTest extends ManagedEntityTestBase {

  /**
   * Test simple instance with no collections to ensure we persist properly
   * without indexing
   */
  @Test
  public void basicCreateRead() {

    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();

    Magazine mag = new Magazine();
    mag.setCopiesSold(10000000);
    mag.setIsbn("11123123");
    mag.setTitle("PC Mag");

    em.persist(mag);

    em.getTransaction().commit();
    em.close();

    EntityManager em2 = entityManagerFactory.createEntityManager();

    Magazine returned = em2.find(Magazine.class,
        new Magazine.MagazineId(mag.getIsbn(), mag.getTitle()));

    assertEquals(mag, returned);
    assertEquals(mag.getCopiesSold(), returned.getCopiesSold());
    assertEquals(mag.getTitle(), returned.getTitle());
    assertEquals(mag.getIsbn(), returned.getIsbn());
  }

  /**
   * Test simple instance with no collections to ensure we persist properly
   * without indexing
   */
  @Test
  public void basicDeleteRead() {

    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();

    Magazine mag = new Magazine();
    mag.setCopiesSold(10000000);
    mag.setIsbn("11123123");
    mag.setTitle("PC Mag");

    em.persist(mag);

    em.getTransaction().commit();
    em.close();

    EntityManager em2 = entityManagerFactory.createEntityManager();

    Magazine returned = em2.find(Magazine.class,
        new Magazine.MagazineId(mag.getIsbn(), mag.getTitle()));

    assertEquals(mag, returned);
    assertEquals(mag.getCopiesSold(), returned.getCopiesSold());
    assertEquals(mag.getTitle(), returned.getTitle());
    assertEquals(mag.getIsbn(), returned.getIsbn());

    em2.getTransaction().begin();
    em2.remove(returned);
    em2.getTransaction().commit();
    em2.close();

    EntityManager em3 = entityManagerFactory.createEntityManager();

    returned = em3.find(Magazine.class, new Magazine.MagazineId(mag.getIsbn(),
        mag.getTitle()));

    assertNull(returned);

  }

}
