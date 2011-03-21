package com.datastax.hectorjpa.meta;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.openjpa.meta.ClassMetaData;

import com.datastax.hectorjpa.store.EntityFacade;
import com.datastax.hectorjpa.store.MappingUtils;


/**
 * Cache for holding all meta data
 * 
 * @author Todd Nine
 *
 */
public class MetaCache {

  
  private final ConcurrentMap<ClassMetaData, EntityFacade> metaData = new ConcurrentHashMap<ClassMetaData, EntityFacade>();
  
  
  private final MappingUtils mappingUtils;
  
  
  /**
   * Create a new meta cache for classes
   * @param mappingUtils
   */
  public MetaCache(MappingUtils mappingUtils){
    this.mappingUtils = mappingUtils;
  }
  
  
  /**
   * Get the entity facade for this class.  If it does not exist it is created and added to the cache
   * @param meta
   * @return
   */
  public EntityFacade getFacade(ClassMetaData meta){
    
   
    EntityFacade facade = metaData.get(meta);
    
    if(facade != null){
      return facade;
    }
    
    facade = new EntityFacade(meta, mappingUtils);
    
    metaData.putIfAbsent(meta, facade);
    
    return facade;
    
  }
  
  
}
