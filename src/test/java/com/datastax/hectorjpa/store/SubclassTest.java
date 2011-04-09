package com.datastax.hectorjpa.store;

import static org.junit.Assert.*;

import javax.persistence.EntityManager;

import org.joda.time.DateTime;
import org.junit.Test;

import com.datastax.hectorjpa.ManagedEntityTestBase;
import com.datastax.hectorjpa.bean.inheritance.SmsMessage;
import com.datastax.hectorjpa.bean.inheritance.VerificationSmsMessage;
import com.datastax.hectorjpa.bean.tree.Nerd;
import com.datastax.hectorjpa.bean.tree.Techie;
import com.eaio.uuid.UUID;

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
public class SubclassTest extends ManagedEntityTestBase {

  /**
   * Test simple instance with no collections to ensure we persist properly
   * without indexing
   */
  @Test
  public void basicLoad() {

    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();

    VerificationSmsMessage verification = new VerificationSmsMessage();

    verification
        .setCreatedDate(new DateTime(2011, 03, 01, 0, 0, 0, 0).toDate());
    verification.setMessageId(10);
    verification.setPhoneNumber("+64111222333");

    em.persist(verification);

    em.getTransaction().commit();
    em.close();

    // verify we get the subclass
    EntityManager em2 = entityManagerFactory.createEntityManager();

    SmsMessage returned = em2.find(SmsMessage.class, verification.getId());

    assertEquals(verification, returned);
    assertEquals(verification.getMessageId(), returned.getMessageId());
    assertEquals(verification.getPhoneNumber(), returned.getPhoneNumber());

    em2.close();

  }
  
  

  /**
   * Test simple instance with no collections to ensure we persist properly
   * without indexing
   */
  @Test
  public void subclassDoesNotLoadSuper() {

    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();

    Techie techie = new Techie();
    techie.setName("testname");
   
    em.persist(techie);

    em.getTransaction().commit();
    em.close();

    // verify we get the subclass
    EntityManager em2 = entityManagerFactory.createEntityManager();

    Object returned = em2.find(Nerd.class, techie.getId());

    assertNull(returned);

    em2.close();

  }

  @Test
  public void abstractClassInstanceTest() {
    EntityManager em = entityManagerFactory.createEntityManager();

    SmsMessage returned = em.find(SmsMessage.class, new UUID());

    assertNull(returned);
  }
  
  @Test
  public void nullDeleteTest() {
    EntityManager em = entityManagerFactory.createEntityManager();

    SmsMessage returned = em.find(SmsMessage.class, null);

    assertNull(returned);
  }

}
