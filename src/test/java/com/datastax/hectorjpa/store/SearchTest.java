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
import com.datastax.hectorjpa.bean.Phone;
import com.datastax.hectorjpa.bean.Phone.PhoneType;
import com.datastax.hectorjpa.bean.Sale;
import com.datastax.hectorjpa.bean.Sale_;
import com.datastax.hectorjpa.bean.Store;
import com.datastax.hectorjpa.bean.inheritance.Client;
import com.datastax.hectorjpa.bean.inheritance.Manager;
import com.datastax.hectorjpa.bean.inheritance.Person;
import com.datastax.hectorjpa.bean.inheritance.Person_;
import com.datastax.hectorjpa.bean.inheritance.User;
import com.datastax.hectorjpa.bean.inheritance.User_;

/**
 * 
 * SETUP NOTES: Unfortunately the open JPA plugin lacks the ability to generate
 * meta data to resolve compilation issues run a test compile from the command
 * line, then include "target/generated-sources/test-annotations" as a source
 * directory in eclipse.
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
    james.setPhoneNumber(new Phone("+641112223333", PhoneType.MOBILE));

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
    // we've asserted everything saved, now time to query the sales

    EntityManager em3 = entityManagerFactory.createEntityManager();

    // See comment in header to get this to build
    CriteriaBuilder queryBuilder = em3.getCriteriaBuilder();

    CriteriaQuery<Sale> query = queryBuilder.createQuery(Sale.class);

    Root<Sale> sale = query.from(Sale.class);

    Predicate predicate = queryBuilder.equal(sale.get(Sale_.itemName),
        jeansSale2.getItemName());

    query.where(predicate);

    Order order = queryBuilder.desc(sale.get(Sale_.sellDate));

    query.orderBy(order);

    TypedQuery<Sale> saleQuery = em3.createQuery(query);

    List<Sale> results = saleQuery.getResultList();

    assertEquals(jeansSale2, results.get(0));

    assertEquals(jeansSale1, results.get(1));

  }

  /**
   * Test simple instance with no collections to ensure we persist properly
   * without indexing
   */
  @Test
  public void subclassSearch() {

    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();

    Person p1 = new Person();
    p1.setEmail("p1@test.com");
    p1.setFirstName("p1");
    p1.setLastName("Person");

    em.persist(p1);

    Person p2 = new Person();

    p2.setEmail(p1.getEmail());
    p2.setFirstName("p2");
    p2.setLastName("Person");

    em.persist(p2);

    User u1 = new User();
    u1.setEmail(p1.getEmail());
    u1.setFirstName("u1");
    u1.setLastName("User");
    u1.setLastLogin(new DateTime(2011, 3, 20, 1, 1, 1, 0).toDate());

    em.persist(u1);

    User u2 = new User();
    u2.setEmail(p1.getEmail());
    u2.setFirstName("u2");
    u2.setLastName("User");
    u2.setLastLogin(new DateTime(2011, 3, 21, 1, 1, 1, 0).toDate());

    em.persist(u2);

    Client c1 = new Client();
    c1.setEmail(p1.getEmail());
    c1.setFirstName("c1");
    c1.setLastName("Client");
    c1.setLastLogin(new DateTime(2011, 3, 22, 1, 1, 1, 0).toDate());

    em.persist(c1);

    Client c2 = new Client();
    c2.setEmail(p1.getEmail());
    c2.setFirstName("c2");
    c2.setLastName("Client");
    c2.setLastLogin(new DateTime(2011, 3, 23, 1, 1, 1, 0).toDate());

    em.persist(c2);

    Manager m1 = new Manager();
    m1.setEmail(p1.getEmail());
    m1.setFirstName("m1");
    m1.setLastName("Manager");
    m1.setLastLogin(new DateTime(2011, 3, 24, 1, 1, 1, 0).toDate());

    em.persist(m1);

    Manager m2 = new Manager();
    m2.setEmail(p1.getEmail());
    m2.setFirstName("m2");
    m2.setLastName("Manager");
    m2.setLastLogin(new DateTime(2011, 3, 25, 1, 1, 1, 0).toDate());

    em.persist(m2);

    em.getTransaction().commit();
    em.close();

    //
    EntityManager em2 = entityManagerFactory.createEntityManager();

    CriteriaBuilder queryBuilder = em2.getCriteriaBuilder();

    CriteriaQuery<Person> query = queryBuilder.createQuery(Person.class);

    Root<Person> person = query.from(Person.class);

    Predicate predicate = queryBuilder.equal(person.get(Person_.email), p1.getEmail());

    query.where(predicate);

    Order fn = queryBuilder.asc(person.get(Person_.firstName));
    Order ln = queryBuilder.asc(person.get(Person_.lastName));

    query.orderBy(fn, ln);

    TypedQuery<Person> personQuery = em2.createQuery(query);

    List<Person> results = personQuery.getResultList();

    assertEquals(8, results.size());

    // client 1 and 2
    assertEquals(c1, results.get(0));
    assertEquals(c2, results.get(1));

    // manager 1 and 2
    assertEquals(m1, results.get(2));
    assertEquals(m2, results.get(3));

    // person 1 and 2
    assertEquals(p1, results.get(4));
    assertEquals(p2, results.get(5));

    // user 1 and 2
    assertEquals(u1, results.get(6));
    assertEquals(u2, results.get(7));

  }
  
  /**
   * Test simple instance with no collections to ensure we persist properly
   * without indexing
   */
  @Test
  public void subclassSearchParentIndex() {

    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();


    User u1 = new User();
    u1.setEmail("user1@foo.com");
    u1.setFirstName("u1");
    u1.setLastName("User");
    u1.setLastLogin(new DateTime(2011, 3, 20, 1, 1, 1, 0).toDate());

    em.persist(u1);

    em.getTransaction().commit();
    em.close();

    //
    EntityManager em2 = entityManagerFactory.createEntityManager();

    CriteriaBuilder queryBuilder = em2.getCriteriaBuilder();

    CriteriaQuery<User> query = queryBuilder.createQuery(User.class);

    Root<User> person = query.from(User.class);

    Predicate predicate = queryBuilder.equal(person.get(User_.email), u1.getEmail());

    query.where(predicate);

    Order fn = queryBuilder.asc(person.get(User_.firstName));
    Order ln = queryBuilder.asc(person.get(User_.lastName));

    query.orderBy(fn, ln);

    TypedQuery<User> userQuery = em2.createQuery(query);

    List<User> results = userQuery.getResultList();

    assertEquals(1, results.size());

    // client 1 and 2
    assertEquals(u1, results.get(0));

  }
  
  /**
   * Tests that we can save an index with one of the fields nulled and still
   * query for null
   * 
   * without indexing
   */
  @Test
  public void nullIndexedField() {

    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();

    Person p1 = new Person();
    p1.setEmail("p1@test.com");
    p1.setFirstName("p1");
    p1.setLastName("Person");

    em.persist(p1);
    

    Client c1 = new Client();
    c1.setEmail(p1.getEmail());
    c1.setFirstName("c1");
    c1.setLastName("Client");

    em.persist(c1);
    
    em.getTransaction().commit();
    em.close();

    //
    EntityManager em2 = entityManagerFactory.createEntityManager();

    CriteriaBuilder queryBuilder = em2.getCriteriaBuilder();

    CriteriaQuery<User> query = queryBuilder.createQuery(User.class);

    Root<User> person = query.from(User.class);

    Predicate predicate = queryBuilder.equal(person.get(User_.lastLogin), null);

    query.where(predicate);

    Order fn = queryBuilder.asc(person.get(Person_.firstName));
    Order ln = queryBuilder.asc(person.get(Person_.lastName));

    query.orderBy(fn, ln);

    TypedQuery<User> saleQuery = em2.createQuery(query);

    List<User> results = saleQuery.getResultList();
    
    assertEquals(1, results.size());
    
    assertEquals(c1, results.get(0));
    

  }

}
