package com.datastax.hectorjpa.meta.collection;

import static org.junit.Assert.assertEquals;

import org.apache.openjpa.persistence.JPAFacadeHelper;
import org.junit.Test;

import com.datastax.hectorjpa.ManagedEntityTestBase;
import com.datastax.hectorjpa.bean.Store;
import com.datastax.hectorjpa.store.MappingUtils;

public class OrderedCollectionFieldTest extends ManagedEntityTestBase {

  @Test
  public void testCollecitonFieldId() {
    OrderedCollectionField<String> orderedCollectionField = new OrderedCollectionField<String>(
        JPAFacadeHelper.getMetaData(entityManagerFactory, Store.class)
            .getDeclaredField("customers"));
    assertEquals(1, orderedCollectionField.getFieldId());
  }
}
