package com.datastax.hectorjpa.query;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.openjpa.kernel.FetchConfiguration;
import org.apache.openjpa.kernel.StoreContext;
import org.apache.openjpa.lib.rop.ResultObjectProvider;
import org.apache.openjpa.lib.rop.ResultObjectProviderIterator;
import org.apache.openjpa.meta.ClassMetaData;

import com.datastax.hectorjpa.store.CassandraClassMetaData;
import com.datastax.hectorjpa.store.CassandraStoreManager;
import com.datastax.hectorjpa.store.MappingUtils;

/**
 * 
 * @author Todd Nine
 * 
 */
public class CassandraResultObjectProvider implements ResultObjectProvider {

  private Set<DynamicComposite> results;
  private StoreContext ctx;
  private int currentIndex;
  private CassandraClassMetaData classMeta;
  private Serializer<Object> idSerializer;
  private FetchConfiguration fetchConfig;
  private Iterator<DynamicComposite> iterator;
  

  public CassandraResultObjectProvider(Set<DynamicComposite> results,
      StoreContext ctx, FetchConfiguration fetchConfig, CassandraClassMetaData classMeta) {
    this.results = results;
    this.ctx = ctx;
    this.currentIndex = -1;
    this.classMeta = classMeta;
    this.fetchConfig = fetchConfig;

    
    idSerializer = MappingUtils.getSerializerForPk(classMeta);
  }

  @Override
  public boolean supportsRandomAccess() {
    return false;
  }

  @Override
  public void open() throws Exception {
    this.iterator = results.iterator();

  }

  @Override
  public Object getResultObject() throws Exception {
    
    DynamicComposite current =  iterator.next();
    
    int length = current.getComponents().size();
   
    
    Object id = current.get(length -1, idSerializer);

    Object jpaId = ctx.newObjectId(classMeta.getDescribedType(), id);

    return ctx.find(jpaId, fetchConfig, null, null, 0);
  }

  @Override
  public boolean next() throws Exception {
    return iterator.hasNext();
  }

  @Override
  public boolean absolute(int pos) throws Exception {
    currentIndex = pos;

    return currentIndex < results.size();

  }

  @Override
  public int size() throws Exception {
    return results.size();
  }

  @Override
  public void reset() throws Exception {
    this.iterator = results.iterator();
  }

  @Override
  public void close() throws Exception {
    // no op

  }

  @Override
  public void handleCheckedException(Exception e) {
    // no op

  }

}
