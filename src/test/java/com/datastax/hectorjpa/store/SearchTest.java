package com.datastax.hectorjpa.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import com.datastax.hectorjpa.bean.*;
import org.joda.time.DateTime;
import org.junit.Ignore;
import org.junit.Test;

import com.datastax.hectorjpa.ManagedEntityTestBase;
import com.datastax.hectorjpa.bean.Phone.PhoneType;
import com.datastax.hectorjpa.bean.inheritance.Client;
import com.datastax.hectorjpa.bean.inheritance.Manager;
import com.datastax.hectorjpa.bean.inheritance.Person;
import com.datastax.hectorjpa.bean.inheritance.Person_;
import com.datastax.hectorjpa.bean.inheritance.UberWarningSmsMessage;
import com.datastax.hectorjpa.bean.inheritance.UberWarningSmsMessage_;
import com.datastax.hectorjpa.bean.inheritance.User;
import com.datastax.hectorjpa.bean.inheritance.User_;
import com.datastax.hectorjpa.bean.inheritance.WarningSmsMessage;
import com.datastax.hectorjpa.bean.inheritance.WarningSmsMessage_;
import com.datastax.hectorjpa.bean.tree.Geek;
import com.datastax.hectorjpa.bean.tree.Geek_;
import com.datastax.hectorjpa.bean.tree.Nerd;
import com.datastax.hectorjpa.bean.tree.Nerd_;
import com.datastax.hectorjpa.bean.tree.Notification;
import com.datastax.hectorjpa.bean.tree.Notification_;
import com.datastax.hectorjpa.bean.tree.Techie;
import com.datastax.hectorjpa.bean.tree.Techie_;
import com.eaio.uuid.UUID;
import com.google.common.collect.Sets;

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
	 * This is an example. In we can't read the identity to load. There must be
	 * an index to perform iteration over
	 */
	@Test
	public void iterateAll() {

		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();

		Store fifth = new Store();
		fifth.setName("5");

		em.persist(fifth);

		Store fourth = new Store();
		fourth.setName("4");

		em.persist(fourth);

		Store third = new Store();
		third.setName("3");

		em.persist(third);

		Store second = new Store();
		second.setName("2");

		em.persist(second);

		Store first = new Store();
		first.setName("1");

		em.persist(first);

		em.getTransaction().commit();

		EntityManager em2 = entityManagerFactory.createEntityManager();
		em2.getTransaction().begin();

		// See comment in header to get this to build
		CriteriaBuilder queryBuilder = em2.getCriteriaBuilder();

		CriteriaQuery<Store> query = queryBuilder.createQuery(Store.class);

		Root<Store> store = query.from(Store.class);

		Predicate start = queryBuilder.greaterThanOrEqualTo(
				store.get(Store_.name), "");
		Predicate end = queryBuilder.lessThan(store.get(Store_.name), "\uffff");

		query.where(start, end);

		TypedQuery<Store> saleQuery = em2.createQuery(query);

		List<Store> results = saleQuery.getResultList();

		assertEquals(first, results.get(0));
		assertEquals(second, results.get(1));
		assertEquals(third, results.get(2));
		assertEquals(fourth, results.get(3));
		assertEquals(fifth, results.get(4));

		queryBuilder = em2.getCriteriaBuilder();

		query = queryBuilder.createQuery(Store.class);

		store = query.from(Store.class);

		start = queryBuilder.greaterThanOrEqualTo(store.get(Store_.name), "");
		end = queryBuilder.lessThan(store.get(Store_.name), "\uffff");

		query.where(start, end);

		saleQuery = em2.createQuery(query);

		saleQuery.setFirstResult(2);
		saleQuery.setMaxResults(2);

		results = saleQuery.getResultList();

		assertEquals(third, results.get(0));
		assertEquals(fourth, results.get(1));

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

		Predicate predicate = queryBuilder.equal(person.get(Person_.email),
				p1.getEmail());

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

		Predicate predicate = queryBuilder.equal(person.get(User_.email),
				u1.getEmail());

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

		Predicate predicate = queryBuilder.equal(person.get(User_.lastLogin),
				null);

		query.where(predicate);

		Order fn = queryBuilder.asc(person.get(Person_.firstName));
		Order ln = queryBuilder.asc(person.get(Person_.lastName));

		query.orderBy(fn, ln);

		TypedQuery<User> saleQuery = em2.createQuery(query);

		List<User> results = saleQuery.getResultList();

		assertEquals(1, results.size());

		assertEquals(c1, results.get(0));

	}

	/**
	 * Tests that we can save an index with one of the fields nulled and still
	 * query for null
	 * 
	 * without indexing
	 */
	@Test
	public void treeTest() {

		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();

		Techie techie = new Techie();
		techie.setName("test");

		Geek g = new Geek();
		g.setName("test");

		Nerd nerd = new Nerd();
		nerd.setName("test");

		em.persist(techie);
		em.persist(g);
		em.persist(nerd);

		em.getTransaction().commit();
		em.close();

		//
		EntityManager em2 = entityManagerFactory.createEntityManager();

		// should only return geek
		CriteriaBuilder geekBuilder = em2.getCriteriaBuilder();

		CriteriaQuery<Geek> geekQuery = geekBuilder.createQuery(Geek.class);

		Root<Geek> geekRoot = geekQuery.from(Geek.class);

		Predicate predicate = geekBuilder.equal(geekRoot.get(Geek_.name),
				g.getName());

		geekQuery.where(predicate);

		TypedQuery<Geek> gQuery = em2.createQuery(geekQuery);

		List<Geek> results = gQuery.getResultList();

		assertEquals(1, results.size());

		assertTrue(results.contains(g));

		// should only return nerd
		CriteriaBuilder nerdBuilder = em2.getCriteriaBuilder();

		CriteriaQuery<Nerd> nerdQuery = nerdBuilder.createQuery(Nerd.class);

		Root<Nerd> nerdRoot = nerdQuery.from(Nerd.class);

		predicate = nerdBuilder.equal(nerdRoot.get(Nerd_.name), g.getName());

		nerdQuery.where(predicate);

		TypedQuery<Nerd> nQuery = em2.createQuery(nerdQuery);

		List<Nerd> nResults = nQuery.getResultList();

		assertEquals(1, nResults.size());

		assertTrue(nResults.contains(nerd));

		// should only return all three
		CriteriaBuilder techieBuilder = em2.getCriteriaBuilder();

		CriteriaQuery<Techie> techieQuery = techieBuilder
				.createQuery(Techie.class);

		Root<Techie> techiRoot = techieQuery.from(Techie.class);

		predicate = techieBuilder.equal(techiRoot.get(Techie_.name),
				g.getName());

		techieQuery.where(predicate);

		TypedQuery<Techie> tQuery = em2.createQuery(techieQuery);

		List<Techie> tResults = tQuery.getResultList();

		assertEquals(3, tResults.size());

		assertTrue(tResults.contains(nerd));
		assertTrue(tResults.contains(g));
		assertTrue(tResults.contains(techie));

	}

	
	/**
	 * Test that we get all results when the number of results is less than the
	 * defined range
	 */
	@Test
	public void searchResultsLessThanDefinedRange() {

		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();

		String name = new UUID().toString();

		Geek g = new Geek();
		g.setName(name);

		Geek g2 = new Geek();
		g2.setName(name);

		em.persist(g);
		em.persist(g2);

		em.getTransaction().commit();
		em.close();

		EntityManager em2 = entityManagerFactory.createEntityManager();

		CriteriaBuilder queryBuilder = em2.getCriteriaBuilder();

		CriteriaQuery<Geek> query = queryBuilder.createQuery(Geek.class);

		Root<Geek> n = query.from(Geek.class);

		Predicate predicate = queryBuilder.equal(n.get(Geek_.name), name);

		query.where(predicate);

		TypedQuery<Geek> geekQuery = em2.createQuery(query);

		geekQuery.setFirstResult(0);
		geekQuery.setMaxResults(100);

		List<Geek> results = geekQuery.getResultList();

		assertEquals(2, results.size());

		assertTrue(results.contains(g));
		assertTrue(results.contains(g2));

	}

	
	@Test
	public void searchNoResults() {

		EntityManager em2 = entityManagerFactory.createEntityManager();

		CriteriaBuilder queryBuilder = em2.getCriteriaBuilder();

		CriteriaQuery<Geek> query = queryBuilder.createQuery(Geek.class);

		Root<Geek> n = query.from(Geek.class);

		Predicate predicate = queryBuilder.equal(n.get(Geek_.name), "blah");

		query.where(predicate);

		TypedQuery<Geek> geekQuery = em2.createQuery(query);

		geekQuery.setFirstResult(0);
		geekQuery.setMaxResults(100);

		List<Geek> results = geekQuery.getResultList();

		assertEquals(0, results.size());

	}
	
	/**
	 * Test that we get all results when the number of results is less than the
	 * defined range
	 */
	@Test
	public void searchResultsExactRange() {

		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();

		String name = new UUID().toString();

		Geek g = new Geek();
		g.setName(name);

		Geek g2 = new Geek();
		g2.setName(name);

		em.persist(g);
		em.persist(g2);

		em.getTransaction().commit();
		em.close();

		EntityManager em2 = entityManagerFactory.createEntityManager();

		CriteriaBuilder queryBuilder = em2.getCriteriaBuilder();

		CriteriaQuery<Geek> query = queryBuilder.createQuery(Geek.class);

		Root<Geek> n = query.from(Geek.class);

		Predicate predicate = queryBuilder.equal(n.get(Geek_.name), name);

		query.where(predicate);

		TypedQuery<Geek> geekQuery = em2.createQuery(query);

		geekQuery.setFirstResult(0);
		geekQuery.setMaxResults(2);

		List<Geek> results = geekQuery.getResultList();

		assertEquals(2, results.size());

		assertTrue(results.contains(g));
		assertTrue(results.contains(g2));

	}
	
	/**
	 * Test that we can page results
	 */
	@Test
	public void searchResultsPaging() {
		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();

		UUID userId = new UUID();

		Date date = new Date();

		for (int i = 0; i < 10; i++) {
			Notification n = new Notification(userId, "message" + i);
			date.setTime(date.getTime() + 1);
			n.setCreatedTime(new Date(date.getTime()));
			em.persist(n);
		}

		em.getTransaction().commit();
		em.close();

		EntityManager em2 = entityManagerFactory.createEntityManager();

		CriteriaBuilder queryBuilder = em2.getCriteriaBuilder();

		CriteriaQuery<Notification> query = queryBuilder
				.createQuery(Notification.class);

		Root<Notification> n = query.from(Notification.class);

		Predicate predicate = queryBuilder.equal(n.get(Notification_.userId),
				userId);

		query.where(predicate);

		Order readOrder = queryBuilder.asc(n.get(Notification_.read));
		Order createdTimeorder = queryBuilder.desc(n.get(Notification_.createdTime));
		query.orderBy(readOrder, createdTimeorder);

		TypedQuery<Notification> q = em2.createQuery(query);

		q.setFirstResult(0);
		q.setMaxResults(5);

		List<Notification> results = q.getResultList();

		assertEquals(5, results.size());
		assertEquals("message9", results.get(0).getMessage());
		assertEquals("message8", results.get(1).getMessage());
		assertEquals("message7", results.get(2).getMessage());
		assertEquals("message6", results.get(3).getMessage());
		assertEquals("message5", results.get(4).getMessage());
		
		
		
		
		em2 = entityManagerFactory.createEntityManager();

		queryBuilder = em2.getCriteriaBuilder();

		query = queryBuilder
				.createQuery(Notification.class);

		n = query.from(Notification.class);

		predicate = queryBuilder.equal(n.get(Notification_.userId),
				userId);

		query.where(predicate);

		readOrder = queryBuilder.asc(n.get(Notification_.read));
		createdTimeorder = queryBuilder.desc(n.get(Notification_.createdTime));
		query.orderBy(readOrder, createdTimeorder);

		q = em2.createQuery(query);

		q.setFirstResult(5);
		q.setMaxResults(5);

		results = q.getResultList();

		assertEquals(5, results.size());
		assertEquals("message4", results.get(0).getMessage());
		assertEquals("message3", results.get(1).getMessage());
		assertEquals("message2", results.get(2).getMessage());
		assertEquals("message1", results.get(3).getMessage());
		assertEquals("message0", results.get(4).getMessage());


	}
	

  /**
   * Tests that when 2 indexes use the same field ordering, they do not conflict.  I.E.
   * 
   * fields=userId, order=read, createdDate desc
   * fields=userId, read, createdDate
   * 
   * These two should not be equal, and should not generate the same row key
   */
  @Test
  public void duplicateFieldOrder() {
    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();

    UUID userId = new UUID();

    
    long startTime = 1313730170000l;
    long time = startTime;

    for (int i = 0; i < 10; i++) {
      Notification n = new Notification(userId, "message" + i);
      n.setCreatedTime(new Date(time));
      n.setRead(i%2==0);
      em.persist(n);
      time+=1000l;
    }

    em.getTransaction().commit();
    em.close();

    EntityManager em2 = entityManagerFactory.createEntityManager();

    CriteriaBuilder queryBuilder = em2.getCriteriaBuilder();

    CriteriaQuery<Notification> query = queryBuilder
        .createQuery(Notification.class);

    Root<Notification> n = query.from(Notification.class);

    Predicate userIdPredicate = queryBuilder.equal(n.get(Notification_.userId),
        userId);
    
    Predicate readPredicate = queryBuilder.equal(n.get(Notification_.read), true);
    
    
    Predicate createdTimePredicate = queryBuilder.greaterThanOrEqualTo(n.get(Notification_.createdTime),  new Date(startTime));
    

    query.where(userIdPredicate, readPredicate, createdTimePredicate);


    TypedQuery<Notification> q = em2.createQuery(query);

    q.setFirstResult(0);
    q.setMaxResults(10);

    List<Notification> results = q.getResultList();

    assertEquals(5, results.size());
    assertEquals("message0", results.get(0).getMessage());
    assertEquals("message2", results.get(1).getMessage());
    assertEquals("message4", results.get(2).getMessage());
    assertEquals("message6", results.get(3).getMessage());
    assertEquals("message8", results.get(4).getMessage());
    
    
    
    
    em2 = entityManagerFactory.createEntityManager();

    queryBuilder = em2.getCriteriaBuilder();

    query = queryBuilder
        .createQuery(Notification.class);

    n = query.from(Notification.class);

    userIdPredicate = queryBuilder.equal(n.get(Notification_.userId),
        userId);

    query.where(userIdPredicate);

    userIdPredicate = queryBuilder.equal(n.get(Notification_.userId),
        userId);
    
    readPredicate = queryBuilder.equal(n.get(Notification_.read), false);
    
    
    createdTimePredicate = queryBuilder.greaterThanOrEqualTo(n.get(Notification_.createdTime),  new Date(startTime));
    

    query.where(userIdPredicate, readPredicate, createdTimePredicate);

    q = em2.createQuery(query);

    q.setFirstResult(0);
    q.setMaxResults(10);

    results = q.getResultList();

    assertEquals(5, results.size());
    assertEquals("message1", results.get(0).getMessage());
    assertEquals("message3", results.get(1).getMessage());
    assertEquals("message5", results.get(2).getMessage());
    assertEquals("message7", results.get(3).getMessage());
    assertEquals("message9", results.get(4).getMessage());


  }
	
	/**
	 * Test that we get last 5 results when the search range overlaps the result
	 * set
	 */
	@Test
	public void searchResultsOverlapingDefinedRange() {

		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();

		UUID userId = new UUID();

		Date date = new Date();

		for (int i = 0; i < 10; i++) {
			Notification n = new Notification(userId, "message" + i);
			date.setTime(date.getTime() + 1);
			n.setCreatedTime(new Date(date.getTime()));
			em.persist(n);
		}

		em.getTransaction().commit();
		em.close();

		EntityManager em2 = entityManagerFactory.createEntityManager();

		CriteriaBuilder queryBuilder = em2.getCriteriaBuilder();

		CriteriaQuery<Notification> query = queryBuilder
				.createQuery(Notification.class);

		Root<Notification> n = query.from(Notification.class);

		Predicate predicate = queryBuilder.equal(n.get(Notification_.userId),
				userId);

		query.where(predicate);

		Order readOrder = queryBuilder.asc(n.get(Notification_.read));
		Order createdTimeorder = queryBuilder.desc(n
				.get(Notification_.createdTime));
		query.orderBy(readOrder, createdTimeorder);

		TypedQuery<Notification> q = em2.createQuery(query);

		q.setFirstResult(5);
		q.setMaxResults(10);

		List<Notification> results = q.getResultList();

		assertEquals(5, results.size());
		assertEquals("message4", results.get(0).getMessage());
		assertEquals("message3", results.get(1).getMessage());
		assertEquals("message2", results.get(2).getMessage());
		assertEquals("message1", results.get(3).getMessage());
		assertEquals("message0", results.get(4).getMessage());
	}

	/**
	 * Test ordering. First order by read and then by createdTime
	 */
	@Test
	public void searchResultsOrdering() {

		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();

		UUID userId = new UUID();

		Date date = new Date();

		for (int i = 0; i < 10; i++) {
			Notification n = new Notification(userId, "message" + i);
			date.setTime(date.getTime() + 1);
			n.setCreatedTime(new Date(date.getTime()));
			n.setRead(i % 2 == 0);
			

			em.persist(n);
		}

		em.getTransaction().commit();
		em.close();

		EntityManager em2 = entityManagerFactory.createEntityManager();

		CriteriaBuilder queryBuilder = em2.getCriteriaBuilder();

		CriteriaQuery<Notification> query = queryBuilder
				.createQuery(Notification.class);

		Root<Notification> n = query.from(Notification.class);

		Predicate predicate = queryBuilder.equal(n.get(Notification_.userId),
				userId);

		query.where(predicate);

		Order readOrder = queryBuilder.asc(n.get(Notification_.read));
		Order createdTimeorder = queryBuilder.desc(n
				.get(Notification_.createdTime));
		query.orderBy(readOrder, createdTimeorder);

		TypedQuery<Notification> q = em2.createQuery(query);

		q.setFirstResult(0);
		q.setMaxResults(10);

		List<Notification> results = q.getResultList();

		assertEquals(10, results.size());
		assertEquals("message9", results.get(0).getMessage());
		assertEquals("message7", results.get(1).getMessage());
		assertEquals("message5", results.get(2).getMessage());
		assertEquals("message3", results.get(3).getMessage());
		assertEquals("message1", results.get(4).getMessage());
		assertEquals("message8", results.get(5).getMessage());
		assertEquals("message6", results.get(6).getMessage());
		assertEquals("message4", results.get(7).getMessage());
		assertEquals("message2", results.get(8).getMessage());
		assertEquals("message0", results.get(9).getMessage());
	}

	@Test
	@Ignore("Or expression currently unsupported.  Waiting on CASSANDRA-2915 to refactor query logic")
	public void queryOrExpression() {

		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();

		String name1 = new UUID().toString();
		String name2 = new UUID().toString();

		Geek g = new Geek();
		g.setName(name1);

		Geek g2 = new Geek();
		g2.setName(name2);

		em.persist(g);
		em.persist(g2);

		em.getTransaction().commit();
		em.close();

		EntityManager em2 = entityManagerFactory.createEntityManager();

		CriteriaBuilder queryBuilder = em2.getCriteriaBuilder();

		CriteriaQuery<Geek> query = queryBuilder.createQuery(Geek.class);

		Root<Geek> n = query.from(Geek.class);

		Predicate predicate = queryBuilder.equal(n.get(Geek_.name), name1);
		Predicate predicate2 = queryBuilder.equal(n.get(Geek_.name), name2);

		query.where(queryBuilder.or(predicate, predicate2));

		TypedQuery<Geek> geekQuery = em2.createQuery(query);

		List<Geek> results = geekQuery.getResultList();

		assertEquals(2, results.size());

		assertTrue(results.contains(g));
		assertTrue(results.contains(g2));
	}

	@Test
	@Ignore("Or expression currently unsupported.  Waiting on CASSANDRA-2915 to refactor query logic")
	public void queryOrExpressionLessThanDefinedRange() {

		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();

		String name1 = new UUID().toString();
		String name2 = new UUID().toString();

		Geek g = new Geek();
		g.setName(name1);

		Geek g2 = new Geek();
		g2.setName(name2);

		em.persist(g);
		em.persist(g2);

		em.getTransaction().commit();
		em.close();

		EntityManager em2 = entityManagerFactory.createEntityManager();

		CriteriaBuilder queryBuilder = em2.getCriteriaBuilder();

		CriteriaQuery<Geek> query = queryBuilder.createQuery(Geek.class);

		Root<Geek> n = query.from(Geek.class);

		Predicate predicate = queryBuilder.equal(n.get(Geek_.name), name1);
		Predicate predicate2 = queryBuilder.equal(n.get(Geek_.name), name2);

		query.where(queryBuilder.or(predicate, predicate2));

		TypedQuery<Geek> geekQuery = em2.createQuery(query);

		geekQuery.setFirstResult(0);
		geekQuery.setMaxResults(100);

		List<Geek> results = geekQuery.getResultList();

		assertEquals(2, results.size());

		assertTrue(results.contains(g));
		assertTrue(results.contains(g2));
	}

	@Test
	@Ignore("Or expression currently unsupported.  Waiting on CASSANDRA-2915 to refactor query logic")
	public void queryOrExpressionOverlappingDefinedRange() {

		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();

		UUID userId = new UUID();
		UUID userId2 = new UUID();

		Date date = new Date();

		for (int i = 0; i < 10; i++) {
			Notification n = new Notification(i % 2 == 0 ? userId : userId2, "message" + i);
			date.setTime(date.getTime() + 1);
			n.setCreatedTime(new Date(date.getTime()));
			em.persist(n);
		}

		em.getTransaction().commit();
		em.close();

		EntityManager em2 = entityManagerFactory.createEntityManager();

		CriteriaBuilder queryBuilder = em2.getCriteriaBuilder();

		CriteriaQuery<Notification> query = queryBuilder
				.createQuery(Notification.class);

		Root<Notification> n = query.from(Notification.class);

		Predicate predicate = queryBuilder.equal(n.get(Notification_.userId), userId);
		Predicate predicate2 = queryBuilder.equal(n.get(Notification_.userId), userId2);

		query.where(queryBuilder.or(predicate, predicate2));

		Order readOrder = queryBuilder.asc(n.get(Notification_.read));
		Order createdTimeorder = queryBuilder.desc(n
				.get(Notification_.createdTime));
		query.orderBy(readOrder, createdTimeorder);

		TypedQuery<Notification> q = em2.createQuery(query);

		q.setFirstResult(5);
		q.setMaxResults(10);

		List<Notification> results = q.getResultList();

		assertEquals(5, results.size());
		

		//NOT SURE WHAT THE RESULTS SHOULD BE FOR OR QUERIES
		assertEquals("message4", results.get(0).getMessage());
		assertEquals("message3", results.get(1).getMessage());
		assertEquals("message2", results.get(2).getMessage());
		assertEquals("message1", results.get(3).getMessage());
		assertEquals("message0", results.get(4).getMessage());
	}

	/**
	 * Make sure when we delete an entity it's removed from the index
	 */
	@Test
	public void deleteCleansIndex() {

		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();

		Store fifth = new Store();
		fifth.setName("deleteCleansIndex-5");

		em.persist(fifth);

		Store fourth = new Store();
		fourth.setName("deleteCleansIndex-4");

		em.persist(fourth);

		Store third = new Store();
		third.setName("deleteCleansIndex-3");

		em.persist(third);

		Store second = new Store();
		second.setName("deleteCleansIndex-2");

		em.persist(second);

		Store first = new Store();
		first.setName("deleteCleansIndex-1");

		em.persist(first);

		em.getTransaction().commit();
		em.close();

		EntityManager em2 = entityManagerFactory.createEntityManager();
		em2.getTransaction().begin();

		// See comment in header to get this to build
		CriteriaBuilder queryBuilder = em2.getCriteriaBuilder();

		CriteriaQuery<Store> query = queryBuilder.createQuery(Store.class);

		Root<Store> store = query.from(Store.class);

		Predicate start = queryBuilder.greaterThanOrEqualTo(
				store.get(Store_.name), "deleteCleansIndex");
		Predicate end = queryBuilder.lessThan(store.get(Store_.name),
				"deleteCleansIndex\uffff");

		query.where(start, end);

		TypedQuery<Store> saleQuery = em2.createQuery(query);

		List<Store> results = saleQuery.getResultList();

		assertEquals(first, results.get(0));
		assertEquals(second, results.get(1));
		assertEquals(third, results.get(2));
		assertEquals(fourth, results.get(3));
		assertEquals(fifth, results.get(4));

		// now delete 1 and 2 and ensure the index is updated.
		em2.remove(results.get(0));
		em2.remove(results.get(1));

		em2.getTransaction().commit();
		em2.close();

		EntityManager em3 = entityManagerFactory.createEntityManager();
		em3.getTransaction().begin();

		// See comment in header to get this to build
		queryBuilder = em3.getCriteriaBuilder();

		query = queryBuilder.createQuery(Store.class);

		store = query.from(Store.class);

		start = queryBuilder.greaterThanOrEqualTo(store.get(Store_.name),
				"deleteCleansIndex");
		end = queryBuilder.lessThan(store.get(Store_.name),
				"deleteCleansIndex\uffff");

		query.where(start, end);

		saleQuery = em3.createQuery(query);

		results = saleQuery.getResultList();

		assertEquals(third, results.get(0));
		assertEquals(fourth, results.get(1));
		assertEquals(fifth, results.get(2));

		em3.getTransaction().commit();
		em3.close();

	}

	/**
	 * Make sure when we delete an entity it's removed from the index
	 */
	@Test
	public void namedQuery() {

		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();

		Store fifth = new Store();
		fifth.setName("namedQuery-5");

		em.persist(fifth);

		Store fourth = new Store();
		fourth.setName("namedQuery-4");

		em.persist(fourth);

		Store third = new Store();
		third.setName("namedQuery-3");

		em.persist(third);
		System.out.println(third.getName());
		Store second = new Store();
		second.setName("namedQuery-2");

		em.persist(second);

		Store first = new Store();
		first.setName("namedQuery-1");

		em.persist(first);
		em.getTransaction().commit();
		em.close();

		EntityManager em2 = entityManagerFactory.createEntityManager();
		em2.getTransaction().begin();

		Query query = em2.createNamedQuery("byname");
		query.setParameter("n", "namedQuery-3");

		Store found = (Store) query.getResultList().get(0);
		assertEquals(third, found);

		em2.getTransaction().commit();
		em2.close();
	}

	/**
	 * Search multiple items with the in operator
	 */
	@Test
	@Ignore
	public void namedQueryWithIn() {

		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();

		Store fifth = new Store();
		fifth.setName("namedQuery-5");

		em.persist(fifth);

		Store fourth = new Store();
		fourth.setName("namedQuery-4");

		em.persist(fourth);

		Store third = new Store();
		third.setName("namedQuery-3");

		em.persist(third);
		System.out.println(third.getName());
		Store second = new Store();
		second.setName("namedQuery-2");

		em.persist(second);

		Store first = new Store();
		first.setName("namedQuery-1");

		em.persist(first);
		em.getTransaction().commit();
		em.close();

		EntityManager em2 = entityManagerFactory.createEntityManager();
		em2.getTransaction().begin();

		final TypedQuery<Store> query = em2.createNamedQuery("inname",
				Store.class);
		final ArrayList<String> names = new ArrayList<String>();
		names.add("namedQuery-3");
		names.add("namedQuery-1");
		query.setParameter("n", names);

		for (final Store found : query.getResultList()) {
			names.remove(found.getName());
		}

		assertTrue(names.isEmpty());

		em2.getTransaction().commit();
		em2.close();
	}

	@Test
	public void searchRangeIncludeMinIncludeMax() {

		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();
		final Set<Integer> others = Sets.newHashSet();

		for (int i = 0; i < 10; i++) {
			Foo1 foo = new Foo1();
			foo.setOther(i);
			em.persist(foo);
			others.add(i);
		}

		em.getTransaction().commit();
		em.close();

		EntityManager em2 = entityManagerFactory.createEntityManager();
		em2.getTransaction().begin();

		TypedQuery<Foo1> query = em2.createNamedQuery(
				"searchRangeIncludeMinIncludeMax", Foo1.class);
		query.setParameter("otherLow", 0);
		query.setParameter("otherHigh", 9);

		for (final Foo1 foo : query.getResultList()) {
			others.remove(foo.getOther());
		}

		if (!others.isEmpty()) {
			fail("Set still contains elements: " + others);
		}

		em2.getTransaction().commit();
		em2.close();
	}

	@Test
	public void searchRangeIncludeMinExcludeMax() {

		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();
		final Set<Integer> others = Sets.newHashSet();

		for (int i = 0; i < 10; i++) {
			Foo1 foo = new Foo1();
			foo.setOther(i);
			em.persist(foo);
			others.add(i);
		}

		em.getTransaction().commit();
		em.close();

		EntityManager em2 = entityManagerFactory.createEntityManager();
		em2.getTransaction().begin();

		TypedQuery<Foo1> query = em2.createNamedQuery(
				"searchRangeIncludeMinExcludeMax", Foo1.class);
		query.setParameter("otherLow", 0);
		query.setParameter("otherHigh", 9);

		for (final Foo1 foo : query.getResultList()) {
			others.remove(foo.getOther());
		}

		assertEquals(1, others.size());
		assertTrue(others.contains(9));

		em2.getTransaction().commit();
		em2.close();
	}

	@Test
	public void searchRangeExcludeMinExcludeMax() {

		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();
		final Set<Integer> others = Sets.newHashSet();

		for (int i = 0; i < 10; i++) {
			Foo1 foo = new Foo1();
			foo.setOther(i);
			em.persist(foo);
			others.add(i);
		}

		em.getTransaction().commit();
		em.close();

		EntityManager em2 = entityManagerFactory.createEntityManager();
		em2.getTransaction().begin();

		TypedQuery<Foo1> query = em2.createNamedQuery(
				"searchRangeExcludeMinExcludeMax", Foo1.class);
		query.setParameter("otherLow", 0);
		query.setParameter("otherHigh", 9);

		for (final Foo1 foo : query.getResultList()) {
			others.remove(foo.getOther());
		}

		assertEquals(2, others.size());
		assertTrue(others.contains(0));
		assertTrue(others.contains(9));
		em2.getTransaction().commit();
		em2.close();
	}

	@Test
	public void searchRangeExcludeMinIncludeMax() {

		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();
		final Set<Integer> others = Sets.newHashSet();

		for (int i = 0; i < 10; i++) {
			Foo1 foo = new Foo1();
			foo.setOther(i);
			em.persist(foo);
			others.add(i);
		}

		em.getTransaction().commit();
		em.close();

		EntityManager em2 = entityManagerFactory.createEntityManager();
		em2.getTransaction().begin();

		TypedQuery<Foo1> query = em2.createNamedQuery(
				"searchRangeExcludeMinIncludeMax", Foo1.class);
		query.setParameter("otherLow", 0);
		query.setParameter("otherHigh", 9);

		for (final Foo1 foo : query.getResultList()) {
			others.remove(foo.getOther());
		}

		assertEquals(1, others.size());
		assertTrue(others.contains(0));
		em2.getTransaction().commit();
		em2.close();
	}

    @Test
    public void searchRangeIncludeMinExcludeMaxWithLong() {
        EntityManager em1 = entityManagerFactory.createEntityManager();
		em1.getTransaction().begin();

        em1.persist(new Foo2(100L));
        em1.persist(new Foo2(200L));
        em1.persist(new Foo2(300L));
        em1.persist(new Foo2(400L));
        em1.persist(new Foo2(500L));

		em1.getTransaction().commit();
		em1.close();

        EntityManager em1request = entityManagerFactory.createEntityManager();
		em1request.getTransaction().begin();

        TypedQuery<Foo2> query = em1request.createNamedQuery(
				"searchRangeIncludeMinExcludeMaxWithLong", Foo2.class);
		query.setParameter("otherLow", 200L);
		query.setParameter("otherHigh", 450L);

        List<Foo2> result1 = query.getResultList();

        System.out.println("result1=" + result1.size());
        assertEquals(3, result1.size());

        em1request.getTransaction().commit();
		em1request.close();

        EntityManager em2 = entityManagerFactory.createEntityManager();
		em2.getTransaction().begin();

        em2.persist(new Foo2(new DateTime(2011, 8, 20, 0, 0, 0, 0).getMillis()));
        em2.persist(new Foo2(new DateTime(2011, 8, 21, 0, 0, 0, 0).getMillis()));
        em2.persist(new Foo2(new DateTime(2011, 8, 22, 0, 0, 0, 0).getMillis()));
        em2.persist(new Foo2(new DateTime(2011, 8, 23, 0, 0, 0, 0).getMillis()));

		em2.getTransaction().commit();
		em2.close();

        EntityManager em2request = entityManagerFactory.createEntityManager();
		em2request.getTransaction().begin();

        TypedQuery<Foo2> query2 = em2request.createNamedQuery(
				"searchRangeIncludeMinExcludeMaxWithLong", Foo2.class);
		query2.setParameter("otherLow", new DateTime(2011, 7, 21, 0, 0, 0, 0).getMillis());
		query2.setParameter("otherHigh", new DateTime(2011, 8, 22, 0, 0, 0, 0).getMillis());

        List<Foo2> result2 = query2.getResultList();

        System.out.println("result2=" + result2.size());
        assertEquals(3, result2.size());

        em2request.getTransaction().commit();
		em2request.close();
    }

	@Test
	public void subclassSearchTest() {

		long start = 1308633528l;

		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();

		String phoneNumber1 = "+64555444333";
		String phoneNumber2 = "+74555444333";
		int messageId = 100;

		UberWarningSmsMessage sms1 = new UberWarningSmsMessage();
		sms1.setCreatedDate(new Date(start));
		sms1.setPhoneNumber(phoneNumber1);
		sms1.setMessageId(messageId);

		em.persist(sms1);

		WarningSmsMessage sms2 = new WarningSmsMessage();
		sms2.setCreatedDate(new Date(start));
		sms2.setPhoneNumber(phoneNumber2);
		sms2.setMessageId(messageId);

		em.persist(sms2);

		em.getTransaction().commit();
		em.close();

		EntityManager em2 = entityManagerFactory.createEntityManager();
		em2.getTransaction().begin();

		// See comment in header to get this to build
		CriteriaBuilder queryBuilder = em2.getCriteriaBuilder();

		CriteriaQuery<WarningSmsMessage> query = queryBuilder
				.createQuery(WarningSmsMessage.class);

		Root<WarningSmsMessage> root = query.from(WarningSmsMessage.class);

		Predicate phonePred = queryBuilder.equal(
				root.get(WarningSmsMessage_.phoneNumber), phoneNumber1);
		Predicate messagePred = queryBuilder.equal(
				root.get(WarningSmsMessage_.messageId), messageId);

		query.where(phonePred, messagePred);

		Order order = queryBuilder.desc(root
				.get(WarningSmsMessage_.createdDate));

		query.orderBy(order);

		TypedQuery<WarningSmsMessage> smsQuery = em2.createQuery(query);

		List<WarningSmsMessage> results = smsQuery.getResultList();

		assertEquals(1, results.size());
		assertEquals(sms1, results.get(0));

		// now create a new query and make sure we get the second value
		queryBuilder = em2.getCriteriaBuilder();

		query = queryBuilder.createQuery(WarningSmsMessage.class);

		root = query.from(WarningSmsMessage.class);

		phonePred = queryBuilder.equal(
				root.get(WarningSmsMessage_.phoneNumber), phoneNumber2);
		messagePred = queryBuilder.equal(
				root.get(WarningSmsMessage_.messageId), messageId);

		query.where(phonePred, messagePred);

		order = queryBuilder.desc(root.get(WarningSmsMessage_.createdDate));

		query.orderBy(order);

		smsQuery = em2.createQuery(query);

		results = smsQuery.getResultList();

		assertEquals(1, results.size());
		assertEquals(sms2, results.get(0));

		// test searching for a child class form WarningSmsMessage, it shoudln't
		// exist

		queryBuilder = em2.getCriteriaBuilder();

		CriteriaQuery<UberWarningSmsMessage> subQuery = queryBuilder
				.createQuery(UberWarningSmsMessage.class);

		Root<UberWarningSmsMessage> subRoot = subQuery
				.from(UberWarningSmsMessage.class);

		Predicate subPhonePred = queryBuilder.equal(
				subRoot.get(UberWarningSmsMessage_.phoneNumber), phoneNumber2);
		Predicate subMessagePred = queryBuilder.equal(
				subRoot.get(UberWarningSmsMessage_.messageId), messageId);

		subQuery.where(subPhonePred, subMessagePred);

		Order subOrder = queryBuilder.desc(subRoot
				.get(UberWarningSmsMessage_.createdDate));

		subQuery.orderBy(subOrder);

		TypedQuery<UberWarningSmsMessage> subSmsQuery = em2
				.createQuery(subQuery);

		List<UberWarningSmsMessage> subResults = subSmsQuery.getResultList();

		assertEquals(0, subResults.size());

		em2.getTransaction().commit();
		em2.close();

	}

	/**
	 * Tests descending order with subclassing
	 */
	@Test
	public void subclassSearchWithDescendingOrderTest() {

		long start1 = 1308633528l;
		long start2 = 2308633528l;

		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();

		String phoneNumber1 = "+94555444333";
		int messageId = 100;

		UberWarningSmsMessage sms1 = new UberWarningSmsMessage();
		sms1.setCreatedDate(new Date(start1));
		sms1.setPhoneNumber(phoneNumber1);
		sms1.setMessageId(messageId);

		em.persist(sms1);

		UberWarningSmsMessage sms2 = new UberWarningSmsMessage();
		sms2.setCreatedDate(new Date(start2));
		sms2.setPhoneNumber(phoneNumber1);
		sms2.setMessageId(messageId);

		em.persist(sms2);

		em.getTransaction().commit();
		em.close();

		EntityManager em2 = entityManagerFactory.createEntityManager();
		em2.getTransaction().begin();

		// See comment in header to get this to build
		CriteriaBuilder queryBuilder = em2.getCriteriaBuilder();

		CriteriaQuery<WarningSmsMessage> query = queryBuilder
				.createQuery(WarningSmsMessage.class);

		Root<WarningSmsMessage> root = query.from(WarningSmsMessage.class);

		Predicate phonePred = queryBuilder.equal(
				root.get(WarningSmsMessage_.phoneNumber), phoneNumber1);
		Predicate messagePred = queryBuilder.equal(
				root.get(WarningSmsMessage_.messageId), messageId);

		query.where(phonePred, messagePred);

		Order order = queryBuilder.desc(root
				.get(WarningSmsMessage_.createdDate));

		query.orderBy(order);

		TypedQuery<WarningSmsMessage> smsQuery = em2.createQuery(query);

		List<WarningSmsMessage> results = smsQuery.getResultList();

		assertEquals(2, results.size());
		assertEquals(sms2, results.get(0));
		assertEquals(sms1, results.get(1));

		em2.getTransaction().commit();
		em2.close();

	}
	
	/**
   * Tests an enumeration collection is saved properly when in a collection
   */
  @Test
  public void bigDecimalTest() {

    long startDate = 1313973045000l;

    Invoice invoiceOne = new Invoice();
    invoiceOne.setAmount(new BigDecimal(BigInteger.valueOf(10000l), 2));
    invoiceOne.setStartDate(new Date(startDate));
    invoiceOne.setEndDate(new Date(startDate + 24 * 60 * 60 * 1000));

    Invoice invoiceTwo = new Invoice();
    invoiceTwo.setAmount(new BigDecimal(BigInteger.valueOf(20000l), 2));
    invoiceTwo.setStartDate(new Date(startDate));
    invoiceTwo.setEndDate(new Date(startDate + 24 * 60 * 60 * 1000));

    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();

    em.persist(invoiceOne);
    em.persist(invoiceTwo);

    em.getTransaction().commit();
    em.close();
    

    EntityManager em2 = entityManagerFactory.createEntityManager();


    CriteriaBuilder queryBuilder = em2.getCriteriaBuilder();

    CriteriaQuery<Invoice> query = queryBuilder.createQuery(Invoice.class);

    Root<Invoice> root = query.from(Invoice.class);

    Predicate amountPred = queryBuilder.greaterThan( root.get(Invoice_.amount), invoiceOne.getAmount());
 
    query.where(amountPred);

    Order order = queryBuilder.desc(root.get(Invoice_.startDate));

    query.orderBy(order);

    TypedQuery<Invoice> smsQuery = em2.createQuery(query);

    List<Invoice> results = smsQuery.getResultList();

    
    int index = results.indexOf(invoiceTwo);
    
    assertTrue(index > -1);

    Invoice returned = results.get(index);

    assertEquals(invoiceTwo.getAmount(), returned.getAmount());
    

    em2.close();


  }
}
