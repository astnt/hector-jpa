package com.datastax.hectorjpa;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.BeforeClass;


public abstract class ManagedEntityTestBase extends CassandraTestBase {
  
  protected static EntityManagerFactory entityManagerFactory;
  
  @BeforeClass
  public static void setup() {
    entityManagerFactory = Persistence.createEntityManagerFactory("openjpa");  
  }
}
