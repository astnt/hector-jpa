/**
 * 
 */
package com.datastax.hectorjpa.service;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


import com.datastax.hectorjpa.store.CassandraStore;

/**
 * Simple implementation of the indexing service that cleans indexes via a
 * blocking queue
 * 
 * @author Todd Nine
 * 
 */
public class InMemoryIndexingService implements IndexingService {

  private static final Logger logger = LoggerFactory
      .getLogger(InMemoryIndexingService.class);

  private static final DynamicCompositeSerializer compositeSerializer = new DynamicCompositeSerializer();

  private static final BytesArraySerializer bytesSerializer = BytesArraySerializer
      .get();

  /**
   * Max number of rows to read at once
   */
  private int MAX_COUNT = 100;

  /**
   * Currently hard coded to allow 1000 pending audits
   */
  private LinkedBlockingQueue<IndexAudit> auditQueue = new LinkedBlockingQueue<IndexAudit>(
      1000);

  /**
   * Currently hard coded to allow 1000 pending audits
   */
  private LinkedBlockingQueue<IndexAudit> deleteQueue = new LinkedBlockingQueue<IndexAudit>(
      1000);

  private CassandraStore store;

  public InMemoryIndexingService(CassandraStore store) {

    this.store = store;

    Thread audit = new Thread(new AuditThread());
    audit.setDaemon(true);
    audit.start();

    Thread delete = new Thread(new DeleteThread());
    delete.setDaemon(true);
    delete.start();
  }

  @Override
  public void audit(IndexAudit audit) {
    auditQueue.offer(audit);
  }

  @Override
  public void delete(IndexAudit audit) {
    deleteQueue.offer(audit);
  }

  /**
   * Delete the id column and the corresponding read column
   * 
   * @param audit
   * @param idComposite
   * @param mutator
   */
  private void deleteColumn(IndexAudit audit, DynamicComposite idComposite,
      Mutator<byte[]> mutator) {
    DynamicComposite readComposite = new DynamicComposite();

    List<Component<?>> component = idComposite.getComponents();

    // add everything from our audit except for the id
    for (int i = 1; i < component.size(); i++) {
      readComposite.add(component.get(i).getBytes());
    }

    // add our id to the end for the delete
    readComposite.add(idComposite.getComponent(0));

    // delete the read column
    mutator.addDeletion(audit.getReadRowKey(), audit.getColumnFamily(),
        readComposite, compositeSerializer, audit.getClock());

    // delete this column
    mutator.addDeletion(audit.getIdRowKey(), audit.getColumnFamily(),
        idComposite, compositeSerializer, audit.getClock());
  }

  private class AuditThread implements Runnable {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void run() {
      try {

        while (true) {
          IndexAudit audit = auditQueue.take();

          // byte[] rowKey = constructKey(MappingUtils.getKeyBytes(objectId),
          // getDefaultSearchmarker());

          SliceQuery<byte[], DynamicComposite, byte[]> query = new ThriftSliceQuery(
              store.getKeyspace(), BytesArraySerializer.get(),
              compositeSerializer, BytesArraySerializer.get());

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

          Mutator<byte[]> mutator = store.createMutator();

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

        }

      } catch (Throwable t) {
        logger.error("Unable to processess audit from queue", t);
        // swallow and continue
      }

    }

  }

  private class DeleteThread implements Runnable {
   
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void run() {
      try {

        while (true) {
          IndexAudit audit = auditQueue.take();

          // byte[] rowKey = constructKey(MappingUtils.getKeyBytes(objectId),
          // getDefaultSearchmarker());

          SliceQuery<byte[], DynamicComposite, byte[]> query = new ThriftSliceQuery(
              store.getKeyspace(), BytesArraySerializer.get(),
              compositeSerializer, BytesArraySerializer.get());

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

          Mutator<byte[]> mutator = store.createMutator();

          do {

            query.setRange(start, end, false, MAX_COUNT);
            query.setKey(audit.getIdRowKey());
            query.setColumnFamily(audit.getColumnFamily());

            slice = query.execute().get();

            for (HColumn<DynamicComposite, byte[]> col : slice.getColumns()) {
              deleteColumn(audit, col.getName(), mutator);

              // reset the start point for the next page
              start = col.getName();
            }

          } while (slice.getColumns().size() == MAX_COUNT);

        }
      } catch (Throwable t) {
        logger.error("Unable to processess audit from queue", t);
        // swallow and continue
      }

    }

  }

}
