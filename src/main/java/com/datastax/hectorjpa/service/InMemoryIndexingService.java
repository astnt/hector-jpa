/**
 * 
 */
package com.datastax.hectorjpa.service;

import static com.datastax.hectorjpa.serializer.CompositeUtils.newComposite;

import java.util.List;

import me.prettyprint.cassandra.model.MutatorImpl;
import me.prettyprint.cassandra.model.thrift.ThriftSliceQuery;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.hector.api.beans.AbstractComposite.Component;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.SliceQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.hectorjpa.store.CassandraStoreConfiguration;

/**
 * Simple implementation of the indexing service that cleans indexes via a
 * blocking queue
 * 
 * @author Todd Nine
 * 
 */
public abstract class InMemoryIndexingService implements IndexingService {

  private static final Logger logger = LoggerFactory
      .getLogger(InMemoryIndexingService.class);

  private static final DynamicCompositeSerializer compositeSerializer = new DynamicCompositeSerializer();

  /**
   * Max number of rows to read at once
   */
  private int MAX_COUNT = 100;

  private CassandraStoreConfiguration config;

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.datastax.hectorjpa.service.IndexingService#postCreate(com.datastax.
   * hectorjpa.store.CassandraStoreConfiguration)
   */
  @Override
  public void postCreate(CassandraStoreConfiguration config) {
    this.config = config;
  }

  /**
   * Delete the id column and the corresponding read column
   * 
   * @param audit
   * @param idComposite
   * @param mutator
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private void deleteColumn(IndexAudit audit, DynamicComposite idComposite,
      Mutator<byte[]> mutator) {

    // delete this column
    mutator.addDeletion(audit.getIdRowKey(), audit.getColumnFamily(),
        idComposite, compositeSerializer, audit.getClock());

    if (audit.isBiDirectional()) {
      DynamicComposite readComposite = newComposite();

      List<Component<?>> component = idComposite.getComponents();

      Component current;

      // add everything from our audit except for the id
      for (int i = 1; i < component.size(); i++) {
        current = component.get(i);

        readComposite.addComponent(current.getValue(), current.getSerializer(),
            current.getComparator());
      }

      current = idComposite.getComponent(0);

      // add our id to the end for the delete
      readComposite.addComponent(current.getValue(), current.getSerializer(),
          current.getComparator());

      // delete the read column
      mutator.addDeletion(audit.getReadRowKey(), audit.getColumnFamily(),
          readComposite, compositeSerializer, audit.getClock());

    }
  }

  /**
   * Perform all audit logic for the given audit
   * 
   * @param audit
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected void auditInternal(IndexAudit audit) {
    SliceQuery<byte[], DynamicComposite, byte[]> query = new ThriftSliceQuery(
        config.getKeyspace(), BytesArraySerializer.get(), compositeSerializer,
        BytesArraySerializer.get());

    DynamicComposite start = audit.getColumnId();

    DynamicComposite end = new DynamicComposite();

    List<Component<?>> startComponents = start.getComponents();

    Component current;

    int i = 0;
    for (; i < startComponents.size() - 1; i++) {
      current = start.getComponent(i);
      end.setComponent(i, current.getValue(), current.getSerializer(),
          current.getComparator(), ComponentEquality.EQUAL);
    }

    current = start.getComponent(i);

    end.setComponent(i, current.getValue(), current.getSerializer(),
        current.getComparator(), ComponentEquality.GREATER_THAN_EQUAL);

    ColumnSlice<DynamicComposite, byte[]> slice = null;

    HColumn<DynamicComposite, byte[]> maxColumn = null;

    Mutator<byte[]> mutator = createMutator();

    do {

      query.setRange(start, end, false, MAX_COUNT);
      query.setKey(audit.getIdRowKey());
      query.setColumnFamily(audit.getColumnFamily());

      slice = query.execute().get();

      for (HColumn<DynamicComposite, byte[]> col : slice.getColumns()) {

        if (maxColumn == null) {
          maxColumn = col;
          continue;
        }

        // our previous max is too old.
        if (col.getClock() > maxColumn.getClock()) {
          deleteColumn(audit, maxColumn.getName(), mutator);
          continue;
        }

        deleteColumn(audit, col.getName(), mutator);

        // reset the start point for the next page
        start = col.getName();
      }

    } while (slice.getColumns().size() == MAX_COUNT);

    mutator.execute();
  }

  /**
   * Perform all deletions for this index
   * 
   * @param audit
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected void deleteInternal(IndexAudit audit) {

    // byte[] rowKey = constructKey(MappingUtils.getKeyBytes(objectId),
    // getDefaultSearchmarker());

    SliceQuery<byte[], DynamicComposite, byte[]> query = new ThriftSliceQuery(
        config.getKeyspace(), BytesArraySerializer.get(), compositeSerializer,
        BytesArraySerializer.get());

    DynamicComposite start = audit.getColumnId();

    DynamicComposite end = new DynamicComposite();

    List<Component<?>> startComponents = start.getComponents();

    Component current;

    int i = 0;
    for (; i < startComponents.size() - 1; i++) {
      current = start.getComponent(i);
      end.setComponent(i, current.getValue(), current.getSerializer(),
          current.getComparator(), ComponentEquality.EQUAL);
    }

    current = start.getComponent(i);

    end.setComponent(i, current.getValue(), current.getSerializer(),
        current.getComparator(), ComponentEquality.GREATER_THAN_EQUAL);

    ColumnSlice<DynamicComposite, byte[]> slice = null;

    HColumn<DynamicComposite, byte[]> maxColumn = null;

    Mutator<byte[]> mutator = createMutator();

    do {

      query.setRange(start, end, false, MAX_COUNT);
      query.setKey(audit.getIdRowKey());
      query.setColumnFamily(audit.getColumnFamily());

      slice = query.execute().get();

      for (HColumn<DynamicComposite, byte[]> col : slice.getColumns()) {

        // our previous max is too old.
        deleteColumn(audit, col.getName(), mutator);

        // reset the start point for the next page
        start = col.getName();
      }

    } while (slice.getColumns().size() == MAX_COUNT);

    mutator.execute();
  }

  private Mutator<byte[]> createMutator() {
    return new MutatorImpl<byte[]>(config.getKeyspace(),
        BytesArraySerializer.get());
  }

}
