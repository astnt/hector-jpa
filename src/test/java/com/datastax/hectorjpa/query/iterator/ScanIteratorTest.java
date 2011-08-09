/**
 * 
 */
package com.datastax.hectorjpa.query.iterator;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.junit.Test;

import com.datastax.hectorjpa.CassandraTestBase;
import com.datastax.hectorjpa.index.AbstractIndexOperation;
import com.datastax.hectorjpa.serializer.TimeUUIDSerializer;
import com.eaio.uuid.UUID;

/**
 * Tests all cases for single scan integration
 * 
 * @author Todd Nine
 * 
 */
public class ScanIteratorTest extends CassandraTestBase {

  private static final byte[] holder = new byte[] { 0 };

  /**
   * Tests the case where start is 0 and the size is within the fetch size
   * window
   */
  @Test
  public void preLoaded() {

    byte[] rowKey = generateComposites(100);

    ScanIterator iterator = new ScanIterator(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), genComposite(100,
            ComponentEquality.GREATER_THAN_EQUAL), rowKey);

    int advanced = iterator.advance(0);

    assertEquals(0, advanced);

    assertNull(iterator.current());

    int loaded = iterator.loadNext(100);

    assertEquals(100, loaded);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(0, value);

  }

  /**
   * Tests the case where start is 0 and the loaded count is greater than
   * cassandra rows window
   */
  @Test
  public void noRecordsLeft() {

    byte[] rowKey = generateComposites(100);

    ScanIterator iterator = new ScanIterator(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), genComposite(100,
            ComponentEquality.GREATER_THAN_EQUAL), rowKey);

    int advanced = iterator.advance(0);

    assertEquals(0, advanced);

    assertNull(iterator.current());

    int loaded = iterator.loadNext(101);

    assertEquals(100, loaded);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(0, value);

  }

  /**
   * Tests the case where start is in the middle of the loaded range and the
   * loaded count is greater than cassandra rows window
   */
  @Test
  public void preloadedAdvance() {

    byte[] rowKey = generateComposites(100);

    ScanIterator iterator = new ScanIterator(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), genComposite(100,
            ComponentEquality.GREATER_THAN_EQUAL), rowKey);

    int advanced = iterator.advance(50);

    assertEquals(50, advanced);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(49, value);

    int loaded = iterator.loadNext(50);

    assertEquals(50, loaded);

    value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(49, value);

    iterator.advance(50);

    value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(99, value);

  }

  /**
   * Tests the case where start is 0 and the loaded count is greater than
   * cassandra rows window
   */
  @Test
  public void preloadedNoRecordsLeft() {

    byte[] rowKey = generateComposites(100);

    ScanIterator iterator = new ScanIterator(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), genComposite(100,
            ComponentEquality.GREATER_THAN_EQUAL), rowKey);

    // advance 0
    int advanced = iterator.advance(50);

    assertEquals(50, advanced);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(49, value);

    int loaded = iterator.loadNext(51);

    assertEquals(50, loaded);

    value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(49, value);

    iterator.advance(50);

    value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(99, value);

  }

  /**
   * Tests the case where start is 0 and the loaded count is greater than
   * cassandra rows window
   */
  @Test
  public void multiPageAdvance() {

    byte[] rowKey = generateComposites(2000);

    ScanIterator iterator = new ScanIterator(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), null, rowKey);

    // advance 0
    int advanced = iterator.advance(1400);

    assertEquals(1400, advanced);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(1399, value);

    // there should only be 200 that are loaded
    int loaded = iterator.loadNext(1000);

    assertEquals(600, loaded);

    value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(1399, value);

  }

  /**
   * Tests the case where start is 0 and the loaded count is greater than
   * cassandra rows window
   */
  @Test
  public void multiPageAdvanceThenLoad() {

    byte[] rowKey = generateComposites(2000);

    ScanIterator iterator = new ScanIterator(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), null, rowKey);

    // advance 0
    int advanced = iterator.advance(1400);

    assertEquals(1400, advanced);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(1399, value);

    // there should only be 200 that are loaded
    int loaded = iterator.loadNext(1000);

    assertEquals(600, loaded);

    value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(1399, value);

    iterator.advance(600);

    value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(1999, value);

  }

  /**
   * Tests the case where we advance to the very end of the set. From there
   * loadNext will return no values
   */
  @Test
  public void multiPageAdvanceUpperLimit() {

    byte[] rowKey = generateComposites(1000);

    ScanIterator iterator = new ScanIterator(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), genComposite(799,
            ComponentEquality.EQUAL), rowKey);

    // advance 800
    int advanced = iterator.advance(800);

    assertEquals(800, advanced);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(799, value);

    // there should only be 200 that are loaded from the saved range, but the
    // upper limit of the range scan restricts the value
    // therefore 0 should be loaded
    int loaded = iterator.loadNext(300);

    assertEquals(0, loaded);

    value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(799, value);

  }

  /**
   * Tests the case where we advance to the very end of the set. From there
   * loadNext will return no values
   */
  @Test
  public void loadWithoutAdvance() {

    byte[] rowKey = generateComposites(1000);

    ScanIterator iterator = new ScanIterator(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), genComposite(799,
            ComponentEquality.EQUAL), rowKey);

    // there should only be 200 that are loaded
    int loaded = iterator.loadNext(1000);

    assertEquals(800, loaded);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(0, value);

  }

  /**
   * Tests the case where we advance to the very end of the set. From there
   * loadNext will return no values
   */
  @Test
  public void loadWithoutAdvanceBeyondRange() {

    byte[] rowKey = generateComposites(1000);

    ScanIterator iterator = new ScanIterator(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), null, rowKey);

    // there should only be 200 that are loaded
    int loaded = iterator.loadNext(2000);

    assertEquals(1000, loaded);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(0, value);

    //can only advance 999 b/c we're already at index 0
    int advanced = iterator.advance(1000);
    
    assertEquals(999, advanced);

    assertNull(iterator.current());
  }

  /**
   * Tests the case where start is 0 and the loaded count is greater than
   * cassandra rows window
   */
  @Test
  public void multiPageLoad() {

    byte[] rowKey = generateComposites(1000);

    ScanIterator iterator = new ScanIterator(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), null, rowKey);

    // advance 0
    int advanced = iterator.advance(0);

    assertEquals(0, advanced);

    assertNull(iterator.current());

    // there should only be 200 that are loaded
    int loaded = iterator.loadNext(2000);

    assertEquals(1000, loaded);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(0, value);

    // advance beyond the possible range
    iterator.advance(2000);

    assertNull(iterator.current());

  }

  private DynamicComposite genComposite(int start, ComponentEquality equality) {
    DynamicComposite composite = new DynamicComposite();
    composite.addComponent(start, IntegerSerializer.get(), IntegerSerializer
        .get().getComparatorType().getTypeName(), equality);

    return composite;
  }

  /**
   * Generate the given number of composites and returns the key for the row
   * 
   * @param size
   */
  private byte[] generateComposites(int size) {

    Mutator<byte[]> mutator = HFactory.createMutator(
        CassandraTestBase.keyspace, BytesArraySerializer.get());
    byte[] rowKey = generateRowKey();

    for (int i = 0; i < size; i++) {

      DynamicComposite composite = new DynamicComposite();
      composite.addComponent(i, IntegerSerializer.get(), IntegerSerializer
          .get().getComparatorType().getTypeName());

      mutator.addInsertion(
          rowKey,
          AbstractIndexOperation.CF_NAME,
          HFactory.createColumn(composite, holder,
              DynamicCompositeSerializer.get(), BytesArraySerializer.get()));
    }

    mutator.execute();

    return rowKey;

  }

  /**
   * Generates a fake row key for testing
   * 
   * @return
   */
  private byte[] generateRowKey() {
    return TimeUUIDSerializer.get().toBytes(new UUID());
  }
}
