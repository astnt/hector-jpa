/**
 * 
 */
package com.datastax.hectorjpa.meta.collection;

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

import com.datastax.hectorjpa.store.MappingUtils;

/**
 * Represents an unordered collection
 * 
 * @author Todd Nine
 * 
 */
public class UnorderedCollectionField<V> extends AbstractCollectionField<V> {

  // represents the end "id" in the key
  private static final byte[] unorderedMarker = StringSerializer.get().toBytes(
      "u");

  public UnorderedCollectionField(FieldMetaData fmd, MappingUtils mappingUtils) {
    super(fmd, mappingUtils);
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

  @Override
  public boolean readField(OpenJPAStateManager stateManager,
      QueryResult<ColumnSlice<DynamicComposite, byte[]>> result) {

    Object[] fields = null;

    StoreContext context = stateManager.getContext();

    // TODO TN use our CollectionProxy here
    Collection<Object> collection = (Collection<Object>) stateManager
        .newProxy(fieldId);

    DynamicComposite dynamicCol = null;

    for (HColumn<DynamicComposite, byte[]> col : result.get().getColumns()) {
      
       // the id will always be the first value in a DynamicComposite type, we
      // only care
      // about that value.
      Object nativeId = col.getName().get(0, this.idSerizlizer);


    Object saved = context.find(context.newObjectId(targetClass, nativeId),
        true, null);


      collection.add(saved);

    }

    // now load all the objects from the ids we were given.

    stateManager.storeObject(fieldId, collection);
    

    return result.get().getColumns().size() > 0;

  }

  @Override
  public void addField(OpenJPAStateManager stateManager,
      Mutator<byte[]> mutator, long clock, byte[] key, String cfName) {

    Object field = stateManager.fetch(fieldId);

    // nothing to do
    if (field == null) {
      return;
    }

    // construct the key
    byte[] idKey = constructKey(key, unorderedMarker);

    writeAdds(stateManager, (Collection<?>)field, mutator, clock, idKey);
    writeDeletes(stateManager, (Collection<?>)field, mutator, clock, idKey);

  }

  /**
   * Remove all indexes for elements
   * @param ctx
   * @param value
   * @param mutator
   * @param clock
   * @param orderKey
   * @param idKey
   * @param cfName
   */
  private void writeDeletes(OpenJPAStateManager stateManager, Collection value,
      Mutator<byte[]> mutator, long clock, byte[] idKey) {

    Collection objects = getRemoved(value);

    if (objects == null) {
      return;
    }

    //TODO TN remove from opposite index 
    
    DynamicComposite idComposite = null;
    Object currentId = null;
    
    StoreContext context = stateManager.getContext();

    // loop through all deleted object and create the deletes for them.
    for (Object current : objects) {

      currentId = mappingUtils.getTargetObject(context.getObjectId(current));

      // create our composite of the format of id+order*
      idComposite = new DynamicComposite();

      // add our id to the beginning of our id based composite
      idComposite.add(currentId, idSerizlizer);

      mutator.addDeletion(idKey, CF_NAME, idComposite, compositeSerializer,
          clock);

    }

  }

  /**
   * Write all indexes for newly added elements
   * @param ctx
   * @param value
   * @param mutator
   * @param clock
   * @param orderKey
   * @param idKey
   * @param cfName
   */
  private void writeAdds(OpenJPAStateManager stateManager, Collection value,
      Mutator<byte[]> mutator, long clock, byte[] idKey) {

    Collection objects = getAdded(value);

    if (objects == null) {
      return;
    }

    DynamicComposite idComposite = null;
    Object currentId = null;
    Object field = null;
    

    StoreContext context = stateManager.getContext();

    // loop through all added objects and create the writes for them.
    for (Object current : objects) {

      currentId = mappingUtils.getTargetObject(context.getObjectId(current));

      // create our composite of the format of id+order*
      idComposite = new DynamicComposite();

      // add our id to the beginning of our id based composite
      idComposite.add(currentId, idSerizlizer);


      mutator.addInsertion(idKey, CF_NAME,
          new HColumnImpl<DynamicComposite, byte[]>(idComposite, HOLDER, clock,
              compositeSerializer, BytesArraySerializer.get()));

    }
  }


  /**
   * Return the collection of deleted objects from the proxy. If none is preset
   * then null is returned
   * 
   * @param field
   * @return
   */
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
  private Collection getAdded(Collection field) {
    if (field instanceof Proxy) {
      return ((Proxy) field).getChangeTracker().getAdded();
    }

    return field;

  }
  

}
