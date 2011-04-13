/**
 * 
 */
package com.datastax.hectorjpa.meta.collection;

import static com.datastax.hectorjpa.serializer.CompositeUtils.getCassType;
import static com.datastax.hectorjpa.serializer.CompositeUtils.newComposite;

import java.nio.ByteBuffer;
import java.util.Collection;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.kernel.StoreContext;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.util.Proxy;

import com.datastax.hectorjpa.service.IndexAudit;
import com.datastax.hectorjpa.service.IndexQueue;

/**
 * Represents an unordered collection
 * 
 * @author Todd Nine
 * 
 */
public class UnorderedCollectionField extends AbstractCollectionField {

  // represents the end "id" in the key
  private static final byte[] unorderedMarker = StringSerializer.get().toBytes(
      "u");

  public UnorderedCollectionField(FieldMetaData fmd) {
    super(fmd);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.datastax.hectorjpa.meta.collection.AbstractCollectionField#
   * getDefaultSearchmarker()
   */
  @Override
  protected byte[] getDefaultSearchmarker() {
    return unorderedMarker;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean readField(OpenJPAStateManager stateManager,
      QueryResult<ColumnSlice<DynamicComposite, byte[]>> result) {


    StoreContext context = stateManager.getContext();

    // TODO TN use our CollectionProxy here
    Collection<Object> collection = (Collection<Object>) stateManager
        .newProxy(fieldId);


    for (HColumn<DynamicComposite, byte[]> col : result.get().getColumns()) {

      // the id will always be the first value in a DynamicComposite type, we
      // only care
      // about that value.
      ByteBuffer buff = col.getName().get(0, buffSerializer);
      
      Object nativeId = elementKeyStrategy.getInstance(buff);

      Object saved = context.find(context.newObjectId(targetClass, nativeId),
          true, null);

      collection.add(saved);

    }

    // now load all the objects from the ids we were given.

    stateManager.store(fieldId, collection);

    return result.get().getColumns().size() > 0;

  }

  @Override
  public void addField(OpenJPAStateManager stateManager,
      Mutator<byte[]> mutator, long clock, byte[] key, String cfName,
      IndexQueue queue) {

    Object field = stateManager.fetch(fieldId);

    byte[] idKey = constructKey(key, unorderedMarker);

    // could have been removed, blitz everything from the index
    if (field == null) {
      mutator.addDeletion(idKey, CF_NAME, null, null);
      return;
    }

    // construct the key

    writeAdds(stateManager, (Collection<?>) field, mutator, clock, idKey, queue);
    writeDeletes(stateManager, (Collection<?>) field, mutator, clock, idKey,
        queue);

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.datastax.hectorjpa.meta.collection.AbstractCollectionField#removeCollection
   * (org.apache.openjpa.kernel.OpenJPAStateManager,
   * me.prettyprint.hector.api.mutation.Mutator, long)
   */
  @Override
  public void removeCollection(OpenJPAStateManager stateManager,
      Mutator<byte[]> mutator, long clock, byte[] key) {

    byte[] idKey = constructKey(key, unorderedMarker);

    mutator.addDeletion(idKey, CF_NAME, null, null);

  }

  /**
   * Remove all indexes for elements
   * 
   * @param ctx
   * @param value
   * @param mutator
   * @param clock
   * @param orderKey
   * @param idKey
   * @param cfName
   */
  @SuppressWarnings("rawtypes")
  private void writeDeletes(OpenJPAStateManager stateManager, Collection value,
      Mutator<byte[]> mutator, long clock, byte[] idKey, IndexQueue queue) {

    Collection objects = getRemoved(value);

    if (objects == null) {
      return;
    }

    // TODO TN remove from opposite index

    DynamicComposite idComposite = null;
    ByteBuffer currentId = null;

    StoreContext context = stateManager.getContext();

    // loop through all deleted object and create the deletes for them.
    for (Object current : objects) {

      currentId = elementKeyStrategy.toByteBuffer(context.getObjectId(current));

      // create our composite of the format of id+order*
      idComposite = newComposite();

      // add our id to the beginning of our id based composite
      idComposite.addComponent(currentId, buffSerializer);

      mutator.addDeletion(idKey, CF_NAME, idComposite, compositeSerializer,
          clock);

      DynamicComposite idAudit = new DynamicComposite();
      idAudit.addComponent(currentId, buffSerializer);

      // add the check to the audit queue
      queue.addDelete(new IndexAudit(idKey, idKey, idAudit, clock, CF_NAME, false));

    }

  }

  /**
   * Write all indexes for newly added elements
   * 
   * @param ctx
   * @param value
   * @param mutator
   * @param clock
   * @param orderKey
   * @param idKey
   * @param cfName
   */
  @SuppressWarnings("rawtypes")
  private void writeAdds(OpenJPAStateManager stateManager, Collection value,
      Mutator<byte[]> mutator, long clock, byte[] idKey, IndexQueue queue) {

    Collection objects = getAdded(value);

    if (objects == null) {
      return;
    }

    DynamicComposite idComposite = null;
    ByteBuffer currentId = null;

    StoreContext context = stateManager.getContext();

    // loop through all added objects and create the writes for them.
    for (Object current : objects) {

      currentId = elementKeyStrategy.toByteBuffer(context.getObjectId(current));

      
      // create our composite of the format of id+order*
      idComposite = newComposite();

      // add our id to the beginning of our id based composite
      idComposite.addComponent(currentId, buffSerializer, getCassType(buffSerializer));

      mutator.addInsertion(idKey, CF_NAME,
          new HColumnImpl<DynamicComposite, byte[]>(idComposite, HOLDER, clock,
              compositeSerializer, BytesArraySerializer.get()));

      DynamicComposite idAudit = new DynamicComposite();
      idAudit.addComponent(currentId, buffSerializer, getCassType(buffSerializer));

      queue.addAudit(new IndexAudit(idKey, idKey, idAudit, clock, CF_NAME, false));
    }
  }

  /**
   * Return the collection of deleted objects from the proxy. If none is preset
   * then null is returned
   * 
   * @param field
   * @return
   */
  @SuppressWarnings("rawtypes")
  private Collection getRemoved(Collection field) {
    if (field instanceof Proxy) {
      return ((Proxy) field).getChangeTracker().getRemoved();
    }

    return null;

  }

  /**
   * Get added values. If the item is not a proxy it is returned as a collection
   * 
   * @param field
   * @return
   */
  @SuppressWarnings("rawtypes")
  private Collection getAdded(Collection field) {
    if (field instanceof Proxy) {
      return ((Proxy) field).getChangeTracker().getAdded();
    }

    return field;

  }

}
