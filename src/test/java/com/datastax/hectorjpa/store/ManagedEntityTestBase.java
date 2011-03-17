package com.datastax.hectorjpa.store;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.BeforeClass;

import com.datastax.hectorjpa.CassandraTestBase;

public abstract class ManagedEntityTestBase extends CassandraTestBase {
  
  protected static EntityManagerFactory entityManagerFactory;
  
  @BeforeClass
  public static void setup() {
    entityManagerFactory = Persistence.createEntityManagerFactory("openjpa");  
  }
}
