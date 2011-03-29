package com.datastax.hectorjpa.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.joda.time.DateTime;
import org.junit.Test;

import com.datastax.hectorjpa.ManagedEntityTestBase;
import com.datastax.hectorjpa.bean.Customer;
import com.datastax.hectorjpa.bean.Sale;
import com.datastax.hectorjpa.bean.Sale_;
import com.datastax.hectorjpa.bean.Store;

/**
 * 
 * SETUP NOTES:  Unfortunately the open JPA plugin lacks the ability to generate meta data
 * to resolve compilation issues run a test compile from the command line, then include
 * "target/generated-sources/test-annotations" as a source directory in eclipse.
 * 
 * Tests basic querying function
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
    
    
    Sale jeansSale1 = new Sale();
    jeansSale1.setItemName("jeans");
    jeansSale1.setSellDate(new DateTime(2011, 1, 1, 0, 0, 0, 0).toDate());
    
    james.addSale(jeansSale1);
    jeansSale1.setCustomer(james);
    
    
    Sale jeansSale2 = new Sale();
    jeansSale2.setItemName("jeans");
    jeansSale2.setSellDate(new DateTime(2011, 1, 4, 0, 0, 0, 0).toDate());
    
    james.addSale(jeansSale2);
    jeansSale2.setCustomer(james);
    
    
    Sale shirtSale = new Sale();
    shirtSale.setItemName("shirt");
    shirtSale.setSellDate(new DateTime(2011, 1, 2, 0, 0, 0, 0).toDate());
    
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
    
    assertTrue(returnedCustomer.getSales().contains(jeansSale1));

    assertTrue(returnedCustomer.getSales().contains(jeansSale2));
    
    assertTrue(returnedCustomer.getSales().contains(shirtSale));
    
    em2.close();
    //we've asserted everything saved, now time to query the sales
    
    EntityManager em3 = entityManagerFactory.createEntityManager();
    
   
    //See comment in header to get this to build
    CriteriaBuilder queryBuilder = em3.getCriteriaBuilder();
    
    CriteriaQuery<Sale> query = queryBuilder.createQuery(Sale.class);
    
    Root<Sale> sale = query.from(Sale.class);
    
    Predicate predicate = queryBuilder.equal(sale.get(Sale_.itemName), jeansSale2.getItemName());
    
    query.where(predicate);
    
    Order order =  queryBuilder.desc(sale.get(Sale_.sellDate));
    
    query.orderBy(order);
    
    TypedQuery<Sale> saleQuery = em3.createQuery(query);
    
    List<Sale> results = saleQuery.getResultList();
    
    
    assertEquals(jeansSale2, results.get(0));
    
    assertEquals(jeansSale1, results.get(1));
    

  }


}
