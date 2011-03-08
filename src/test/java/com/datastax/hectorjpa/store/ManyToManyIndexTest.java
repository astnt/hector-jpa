package com.datastax.hectorjpa.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import javax.persistence.EntityManager;
import javax.persistence.EntityNotFoundException;

import org.junit.Test;

import com.datastax.hectorjpa.bean.User;

/**
 * Test many to many indexing through an object graph. While many-to-many in
 * theory, in practice this is actually 2 one-to-many bi-directional
 * relationships.
 * 
 * @author Todd Nine
 * 
 */
public class ManyToManyIndexTest extends ManagedEntityTestBase {

  /**
   * Test simple instance with no collections to ensure we persist properly
   * without indexing
   */
  @Test
  public void basicCasePersistence() {

    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();

    User user = new User();
    user.setFirstName("Test");
    user.setLastName("User");
    user.setEmail("test.user@testing.com");

    em.persist(user);
    em.getTransaction().commit();
    em.close();

    EntityManager em2 = entityManagerFactory.createEntityManager();
    User returnedUser = em2.find(User.class, user.getId());

    assertEquals(user, returnedUser);

    assertEquals(user.getFirstName(), returnedUser.getFirstName());
    assertEquals(user.getLastName(), returnedUser.getLastName());
    assertEquals(user.getEmail(), returnedUser.getEmail());

    em2.getTransaction().begin();

    em2.remove(returnedUser);
    em2.getTransaction().commit();
    em2.close();

    EntityManager em3 = entityManagerFactory.createEntityManager();

    boolean found = true;

    try {
      em3.find(User.class, user.getId());
    } catch (EntityNotFoundException enfe) {
      found = false;
    }

    assertFalse(found);

  }

}
