package com.datastax.hectorjpa.store;

import static org.junit.Assert.*;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.hectorjpa.ManagedEntityTestBase;
import com.datastax.hectorjpa.bean.SimpleTestBean;

public class StandaloneEntityManagerFactoryTest extends ManagedEntityTestBase {
  
  @Test
  public void testBuildEntityManagerFactory() {
    
    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();
    em.persist(new SimpleTestBean(1, "foo"));
    em.getTransaction().commit();
    em.close();
    
    //em.getTransaction().begin();
    
    EntityManager em2 = entityManagerFactory.createEntityManager();
    SimpleTestBean stb = em2.find(SimpleTestBean.class, 1);
    //em.getTransaction().commit();
    assertEquals("foo",stb.getName());
    

    em2.getTransaction().begin();
    em2.remove(stb);
    em2.getTransaction().commit();
    em2.close();
  }  

}
