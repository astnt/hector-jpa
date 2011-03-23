package com.datastax.hectorjpa.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import javax.persistence.EntityManager;

import org.junit.Test;

import com.datastax.hectorjpa.ManagedEntityTestBase;
import com.datastax.hectorjpa.bean.Customer;
import com.datastax.hectorjpa.bean.Store;

/**
 * Test many to many indexing through an object graph. While many-to-many in
 * theory, in practice this is actually 2 one-to-many bi-directional
 * relationships.
 * 
 * @author Todd Nine
 * 
 */
public class OneToManyIndexTest extends ManagedEntityTestBase {

  /**
   * Test simple instance with no collections to ensure we persist properly
   * without indexing
   */
  @Test
  public void basicCasePersistence() {

    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();

    Store store = new Store();
    store.setName("Manhatten");

    Customer james = new Customer();
    james.setEmail("james@test.com");
    james.setName("James");
    james.setPhoneNumber("+641112223333");

    Customer luke = new Customer();
    luke.setEmail("luke@test.com");
    luke.setName("Luke");
    luke.setPhoneNumber("+64111222333");

    store.addCustomer(james);
    store.addCustomer(luke);

    em.persist(store);
    em.getTransaction().commit();
    em.close();

    EntityManager em2 = entityManagerFactory.createEntityManager();

    Store returnedStore = em2.find(Store.class, store.getId());

    /**
     * Make sure the stores are equal and everything is in sorted order
     */
    assertEquals(store, returnedStore);

    assertEquals(james, returnedStore.getCustomers().get(0));

    assertEquals(luke, returnedStore.getCustomers().get(1));

  }

  /**
   * Delete the first index and make sure it's removed
   */
  @Test
  public void basicDelete() {

    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();

    Store store = new Store();
    store.setName("Manhatten");

    Customer james = new Customer();
    james.setEmail("james@test.com");
    james.setName("James");
    james.setPhoneNumber("+641112223333");

    Customer luke = new Customer();
    luke.setEmail("luke@test.com");
    luke.setName("Luke");
    luke.setPhoneNumber("+64111222333");

    store.addCustomer(james);
    store.addCustomer(luke);

    em.persist(store);
    em.getTransaction().commit();
    em.close();

    EntityManager em2 = entityManagerFactory.createEntityManager();

    //now delete james and make sure it's not returned.
    em2.getTransaction().begin();
    
    Store returnedStore = em2.find(Store.class, store.getId());

    /**
     * Make sure the stores are equal and everything is in sorted order
     */
    assertEquals(store, returnedStore);

    assertEquals(james, returnedStore.getCustomers().get(0));

    assertEquals(luke, returnedStore.getCustomers().get(1));
    
    //remove james
    returnedStore.getCustomers().remove(0);
    
    em2.persist(returnedStore);
    em2.getTransaction().commit();
    
    //now create a new em and pull the value out
    EntityManager em3 = entityManagerFactory.createEntityManager();
    
    returnedStore = em3.find(Store.class, store.getId());
    
    assertEquals(store, returnedStore);

    assertEquals(1, returnedStore.getCustomers().size());
    
    assertEquals(luke, returnedStore.getCustomers().get(0));
    

  }

}
