package com.datastax.hectorjpa.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import javax.persistence.EntityManager;

import org.joda.time.DateTime;
import org.junit.Test;

import com.datastax.hectorjpa.ManagedEntityTestBase;
import com.datastax.hectorjpa.bean.Customer;
import com.datastax.hectorjpa.bean.Sale;
import com.datastax.hectorjpa.bean.Store;

/**
 * Test many to many indexing through an object graph. While many-to-many in
 * theory, in practice this is actually 2 one-to-many bi-directional
 * relationships.
 * 
 * @author Todd Nine
 * 
 */
public class SearchTest extends ManagedEntityTestBase {

  /**
   * Test simple instance with no collections to ensure we persist properly
   * without indexing
   */
  @Test
  public void basicSearch() {

    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();

    Store store = new Store();
    store.setName("Manhattan");

    Customer james = new Customer();
    james.setEmail("james@test.com");
    james.setName("James");
    james.setPhoneNumber("+641112223333");

   

    store.addCustomer(james);
    
    
    Sale jeansSale = new Sale();
    jeansSale.setItemName("jeans");
    jeansSale.setSellDate(new DateTime(2011, 1, 1, 0, 0, 0, 0));
    
    james.addSale(jeansSale);
    jeansSale.setCustomer(james);
    
    
    Sale shirtSale = new Sale();
    shirtSale.setItemName("shirt");
    shirtSale.setSellDate(new DateTime(2011, 1, 2, 0, 0, 0, 0));
    
    james.addSale(shirtSale);
    shirtSale.setCustomer(james);
    

    em.persist(store);
    em.getTransaction().commit();
    em.close();

    EntityManager em2 = entityManagerFactory.createEntityManager();

    Store returnedStore = em2.find(Store.class, store.getId());

    /**
     * Make sure the stores are equal and everything is in sorted order
     */
    assertEquals(store, returnedStore);
    
    Customer returnedCustomer = returnedStore.getCustomers().get(0);

    assertEquals(james, returnedCustomer);

    assertEquals(jeansSale, returnedCustomer.getSales().get(0));
    
    assertEquals(shirtSale, returnedCustomer.getSales().get(1));
    
    

  }


}
