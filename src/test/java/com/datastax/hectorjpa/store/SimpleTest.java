package com.datastax.hectorjpa.store;

import static org.junit.Assert.assertEquals;

import javax.persistence.EntityManager;

import org.junit.Test;

import com.datastax.hectorjpa.ManagedEntityTestBase;
import com.datastax.hectorjpa.bean.PrimitiveTypes;
import com.datastax.hectorjpa.bean.SimpleTestBean;
import com.datastax.hectorjpa.bean.PrimitiveTypes.TestEnum;

public class SimpleTest extends ManagedEntityTestBase {
  
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
  
  @Test
  public void testPrimitives() {
    PrimitiveTypes saved = new PrimitiveTypes();
    saved.setBoolValue(true);
    saved.setCharValue('\uffff');
    saved.setShortVal((short) 256);
    saved.setIntVal(10000);
    saved.setDoubleVal(.00023);
    saved.setLongVal(200000000l);
    saved.setFloatVal(22.0029292f);
    saved.setTestEnum(TestEnum.TWO);
        
    
    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();
    em.persist(saved);
    em.getTransaction().commit();
    em.close();
    
    //em.getTransaction().begin();
    
    EntityManager em2 = entityManagerFactory.createEntityManager();
    
    PrimitiveTypes returned = em2.find(PrimitiveTypes.class, saved.getId());
   
    //em.getTransaction().commit();
    assertEquals(saved,returned);
    assertEquals(saved.isBoolValue(), returned.isBoolValue());
    assertEquals(saved.getCharValue(), returned.getCharValue());
    assertEquals(saved.getShortVal(), returned.getShortVal());
    assertEquals(saved.getIntVal(), returned.getIntVal());
    assertEquals(saved.getDoubleVal(), returned.getDoubleVal(), 0);
    assertEquals(saved.getLongVal(), returned.getLongVal());
    assertEquals(saved.getFloatVal(), returned.getFloatVal(), 0);
    assertEquals(saved.getTestEnum(), returned.getTestEnum());
    

    em2.close();
  }  


}
