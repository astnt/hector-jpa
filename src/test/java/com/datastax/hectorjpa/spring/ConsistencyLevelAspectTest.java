/**
 * 
 */
package com.datastax.hectorjpa.spring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import me.prettyprint.hector.api.HConsistencyLevel;

import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.datastax.hectorjpa.consitency.JPAConsistency;
import com.datastax.hectorjpa.spring.model.Model;
import com.datastax.hectorjpa.spring.model.ModelChild;
import com.datastax.hectorjpa.spring.service.ComplexService;
import com.datastax.hectorjpa.spring.service.SimpleService;
import com.datastax.hectorjpa.spring.service.SimpleServiceTwo;

/**
 * @author Todd Nine
 * 
 */
public class ConsistencyLevelAspectTest {

  private ApplicationContext context;

  /**
   * Re-init the app context to reset state after every test
   * 
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    context = new ClassPathXmlApplicationContext("classpath:/test-context.xml");
  }

  @Test
  public void singleInvocation() {
    SimpleService service = context.getBean(SimpleService.class);

    service.doOp(new Model());

    assertTrue(service.isDoOp());
  }

  @Test
  public void parentNestedInvocation() {
    SimpleService service = context.getBean(SimpleService.class);
    SimpleServiceTwo serviceTwo = context.getBean(SimpleServiceTwo.class);
    ComplexService complex = context.getBean(ComplexService.class);

    complex.doOp(new Model());

    assertFalse(complex.isDoOpChild());
    assertTrue(complex.isDoOp());
    assertTrue(service.isDoOp());
    assertTrue(serviceTwo.isDoOp());

  }

  @Test
  public void parentNestedInvocationWithCache() {
    SimpleService service = context.getBean(SimpleService.class);
    SimpleServiceTwo serviceTwo = context.getBean(SimpleServiceTwo.class);
    ComplexService complex = context.getBean(ComplexService.class);

    complex.doOp(new Model());

    assertFalse(complex.isDoOpChild());
    assertTrue(complex.isDoOp());
    assertTrue(service.isDoOp());
    assertTrue(serviceTwo.isDoOp());

    complex.doOp(new Model());

    assertFalse(complex.isDoOpChild());
    assertTrue(complex.isDoOp());
    assertTrue(service.isDoOp());
    assertTrue(serviceTwo.isDoOp());

  }

  @Test
  public void childNestedInvocation() {
    SimpleService service = context.getBean(SimpleService.class);
    SimpleServiceTwo serviceTwo = context.getBean(SimpleServiceTwo.class);
    ComplexService complex = context.getBean(ComplexService.class);

    complex.doOp(new ModelChild());

    assertFalse(complex.isDoOp());
    assertTrue(complex.isDoOpChild());
    assertTrue(service.isDoOp());
    assertTrue(serviceTwo.isDoOp());

  }

  @Test
  public void childNestedInvocationWithException() {
    
    //not realistic, but used for testing
    HConsistencyLevel defaultCl = HConsistencyLevel.ALL;
    
    JPAConsistency.setDefault(defaultCl);
    
    ComplexService complex = context.getBean(ComplexService.class);

    try {
      complex.doOpChildException(new ModelChild());
    } catch (RuntimeException re) {
      // ensure our state is rest after the exception is thrown
      assertEquals(defaultCl, JPAConsistency.get());

      return;
    }

    fail("Runtime exception should have been thrown");

  }

}
