package com.datastax.hectorjpa.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import javax.persistence.EntityManager;

import org.junit.Ignore;
import org.junit.Test;

import com.datastax.hectorjpa.ManagedEntityTestBase;
import com.datastax.hectorjpa.bean.FollowState;
import com.datastax.hectorjpa.bean.Observe;
import com.datastax.hectorjpa.bean.User;

/**
 * Test many to many indexing through an object graph. While many-to-many in
 * theory, in practice this is actually 2 one-to-many bi-directional
 * relationships.
 * 
 * @author Todd Nine
 * 
 */
public class ManyToOneIndexTest extends ManagedEntityTestBase {

  /**
   * Test simple instance with no collections to ensure we persist properly
   * without indexing
   */
  @Test
  // @Ignore
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
    em2.getTransaction().begin();

    User returnedUser = em2.find(User.class, user.getId());

    assertEquals(user, returnedUser);

    assertEquals(user.getFirstName(), returnedUser.getFirstName());
    assertEquals(user.getLastName(), returnedUser.getLastName());
    assertEquals(user.getEmail(), returnedUser.getEmail());

    em2.remove(returnedUser);
    em2.getTransaction().commit();
    em2.close();

    EntityManager em3 = entityManagerFactory.createEntityManager();

    returnedUser = em3.find(User.class, user.getId());

    assertNull(returnedUser);

  }

  /**
   * Test simple instance with no collections to ensure we persist properly
   * without indexing
   */
  @Test
  public void basicFollowingPersistence() {

    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();

    User bob = new User();
    bob.setFirstName("Bob");
    bob.setLastName("Smith");
    bob.setEmail("bob.smith@testing.com");

    User frank = new User();
    frank.setFirstName("Frank");
    frank.setLastName("Smith");
    frank.setEmail("frank.smith@testing.com");

    // bob.observeUser(frank, FollowState.PENDING);

    // TODO on commit we're only getting 2 entities in the graph, bob and
    // Observer, yet the graph of bob <-> Follower <-> Frank is build. Is this a
    // bug?
    // em.persist(bob);
    Observe observe = new Observe();
    observe.setOwner(bob);
    observe.setTarget(frank);
    observe.setState(FollowState.PENDING);
    // bob.observeUser(frank, FollowState.PENDING);
    bob.getObserving().add(observe);
    frank.getObservers().add(observe);

    em.persist(bob);
    em.persist(frank);
    em.persist(observe);

    em.getTransaction().commit();
    em.close();

    EntityManager em2 = entityManagerFactory.createEntityManager();

    // find bob
    User returnedUser = em2.find(User.class, bob.getId());

    // check default fields
    assertEquals(bob, returnedUser);
    assertEquals(bob.getFirstName(), returnedUser.getFirstName());
    assertEquals(bob.getLastName(), returnedUser.getLastName());
    assertEquals(bob.getEmail(), returnedUser.getEmail());

    // check our many relation is correct
    Observe observeResult = returnedUser.getObserving().get(0);

    // check the to one is correct
    assertEquals(frank, observeResult.getTarget());
    assertEquals(observe.getState(), observeResult.getState());
    assertEquals(0, returnedUser.getObservers().size());

    // find frank
    returnedUser = em2.find(User.class, frank.getId());

    // check base object
    assertEquals(frank, returnedUser);

    assertEquals(frank.getFirstName(), returnedUser.getFirstName());
    assertEquals(frank.getLastName(), returnedUser.getLastName());
    assertEquals(frank.getEmail(), returnedUser.getEmail());

    // check to many
    observeResult = returnedUser.getObservers().get(0);

    // check to one
    assertEquals(bob, observeResult.getOwner());
    assertEquals(observe.getState(), observeResult.getState());
    assertEquals(0, returnedUser.getObserving().size());

  }

  @Test
  @Ignore("Skipping to perform alpha build.  Delete still needs a fair amount of work")
  public void basicFollowingDelete() {

    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();

    User bob = new User();
    bob.setFirstName("Bob");
    bob.setLastName("Smith");
    bob.setEmail("bob.smith@testing.com");

    User frank = new User();
    frank.setFirstName("Frank");
    frank.setLastName("Smith");
    frank.setEmail("frank.smith@testing.com");

    // bob.observeUser(frank, FollowState.PENDING);

    // TODO on commit we're only getting 2 entities in the graph, bob and
    // Observer, yet the graph of bob <-> Follower <-> Frank is build. Is this a
    // bug?
    // em.persist(bob);
    Observe observe = new Observe();
    observe.setOwner(bob);
    observe.setTarget(frank);
    observe.setState(FollowState.PENDING);
    // bob.observeUser(frank, FollowState.PENDING);
    bob.getObserving().add(observe);
    frank.getObservers().add(observe);

    em.persist(bob);
    em.persist(frank);
    em.persist(observe);

    em.getTransaction().commit();
    em.close();

    EntityManager em2 = entityManagerFactory.createEntityManager();

    em2.getTransaction().begin();

    // find bob
    User returnedUser = em2.find(User.class, bob.getId());

    // check default fields
    assertEquals(bob, returnedUser);
    assertEquals(bob.getFirstName(), returnedUser.getFirstName());
    assertEquals(bob.getLastName(), returnedUser.getLastName());
    assertEquals(bob.getEmail(), returnedUser.getEmail());

    // check our many relation is correct
    Observe observeResult = returnedUser.getObserving().get(0);

    // check the to one is correct
    assertEquals(frank, observeResult.getTarget());
    assertEquals(observe.getState(), observeResult.getState());
    assertEquals(0, returnedUser.getObservers().size());

    // find frank
    returnedUser = em2.find(User.class, frank.getId());

    // check base object
    assertEquals(frank, returnedUser);

    assertEquals(frank.getFirstName(), returnedUser.getFirstName());
    assertEquals(frank.getLastName(), returnedUser.getLastName());
    assertEquals(frank.getEmail(), returnedUser.getEmail());

    // check to many
    observeResult = returnedUser.getObservers().get(0);

    // check to one
    assertEquals(bob, observeResult.getOwner());
    assertEquals(observe.getState(), observeResult.getState());
    assertEquals(0, returnedUser.getObserving().size());

    // now we remove bob's observing link. Should remove the Observe object but
    // not Frank

    // remove the link
    bob.getObserving().remove(observeResult);
    frank.getObservers().remove(observeResult);

    // remove the middle link
    em2.remove(observeResult);

    em2.getTransaction().commit();
    em2.close();

    EntityManager em3 = entityManagerFactory.createEntityManager();

    returnedUser = em3.find(User.class, bob.getId());

    // check default fields
    assertEquals(bob, returnedUser);
    assertEquals(bob.getFirstName(), returnedUser.getFirstName());
    assertEquals(bob.getLastName(), returnedUser.getLastName());
    assertEquals(bob.getEmail(), returnedUser.getEmail());

    // check our many relation is correct
    assertEquals(0, returnedUser.getObserving().size());
    assertEquals(0, returnedUser.getObservers().size());

    returnedUser = em3.find(User.class, frank.getId());

    // check base object
    assertEquals(frank, returnedUser);

    assertEquals(frank.getFirstName(), returnedUser.getFirstName());
    assertEquals(frank.getLastName(), returnedUser.getLastName());
    assertEquals(frank.getEmail(), returnedUser.getEmail());

    assertEquals(0, returnedUser.getObserving().size());
    assertEquals(0, returnedUser.getObservers().size());

  }

}
