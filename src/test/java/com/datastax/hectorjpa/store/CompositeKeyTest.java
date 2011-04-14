package com.datastax.hectorjpa.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.junit.Test;

import com.datastax.hectorjpa.ManagedEntityTestBase;
import com.datastax.hectorjpa.bean.Magazine;
import com.datastax.hectorjpa.bean.Magazine_;
import com.eaio.uuid.UUID;

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

  /**
   * Test simple instance with no collections to ensure we persist properly
   * without indexing
   */
  @Test
  public void basicQuery() {

    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();

    Magazine mag = new Magazine();
    mag.setCopiesSold(10000000);
    mag.setIsbn("11123123");
    mag.setTitle(new UUID().toString());

    em.persist(mag);

    em.getTransaction().commit();
    em.close();

    EntityManager em2 = entityManagerFactory.createEntityManager();

    CriteriaBuilder queryBuilder = em2.getCriteriaBuilder();

    CriteriaQuery<Magazine> query = queryBuilder.createQuery(Magazine.class);

    Root<Magazine> magazineroot = query.from(Magazine.class);

    // both phonenumber and message id need to be equal
    Predicate titlePredicate = queryBuilder.equal(
        magazineroot.get(Magazine_.title), mag.getTitle());

    query.where(titlePredicate);

    TypedQuery<Magazine> contactQuery = em2.createQuery(query);

    List<Magazine> mags = contactQuery.getResultList();

    assertEquals(1, mags.size());

    Magazine returned = mags.get(0);

    assertEquals(mag, returned);
    assertEquals(mag.getCopiesSold(), returned.getCopiesSold());
    assertEquals(mag.getTitle(), returned.getTitle());
    assertEquals(mag.getIsbn(), returned.getIsbn());

    em2.getTransaction().begin();
    em2.remove(returned);
    em2.getTransaction().commit();
    em2.close();

    EntityManager em3 = entityManagerFactory.createEntityManager();

    queryBuilder = em3.getCriteriaBuilder();

    query = queryBuilder.createQuery(Magazine.class);

    magazineroot = query.from(Magazine.class);

    // both phonenumber and message id need to be equal
    titlePredicate = queryBuilder.equal(magazineroot.get(Magazine_.title),
        mag.getTitle());

    query.where(titlePredicate);

    contactQuery = em3.createQuery(query);

    mags = contactQuery.getResultList();

    assertEquals(0, mags.size());
    
    em3.close();

  }

}
