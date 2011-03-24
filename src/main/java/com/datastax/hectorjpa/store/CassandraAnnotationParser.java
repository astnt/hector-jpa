/**
 * 
 */
package com.datastax.hectorjpa.store;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.HashMap;
import java.util.Map;

import org.apache.openjpa.conf.OpenJPAConfiguration;
import org.apache.openjpa.jdbc.meta.ClassMapping;
import org.apache.openjpa.jdbc.meta.FieldMapping;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.persistence.AnnotationPersistenceMetaDataParser;

import com.datastax.hectorjpa.annotation.Index;
import com.datastax.hectorjpa.index.IndexDefinitions;

/**
 * Annotation parser to handle custom annotations for cassandra
 * 
 * @author Todd Nine
 * 
 */
public class CassandraAnnotationParser extends
    AnnotationPersistenceMetaDataParser {

  private static final Map<Class<?>, ClassMapping> mapping = new HashMap<Class<?>, ClassMapping>();

  static {
    mapping.put(Index.class, ClassMapping.INDEX);
  }

  public CassandraAnnotationParser(OpenJPAConfiguration conf) {
    super(conf);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.openjpa.persistence.AnnotationPersistenceMetaDataParser#
   * parseMemberMappingAnnotations(org.apache.openjpa.meta.FieldMetaData)
   */
  @Override
  protected void parseMemberMappingAnnotations(FieldMetaData fmd) {

    AnnotatedElement el = (AnnotatedElement) getRepository()
        .getMetaDataFactory().getDefaults().getBackingMember(fmd);

    ClassMapping mapped = null;

    for (Annotation annotation : el.getDeclaredAnnotations()) {
      mapped = mapping.get(annotation.annotationType());
      
      if(mapped == null){
        continue;
      }

      switch (mapped) {
      
      case INDEX:
        handleIndex(fmd, (Index) annotation);
        break;
      }

    }

//    super.parseMemberMappingAnnotations(fmd);
  }

  /**
   * Parse the cassandra index expression
   * @param fmd
   * @param index
   */
  private void handleIndex(FieldMetaData fmd, Index index) {
    
    String orderClause = index.value();
    
    IndexDefinitions defs = (IndexDefinitions) fmd.getObjectExtension(IndexDefinitions.EXT_NAME);
    
    if(defs == null){
      defs = new IndexDefinitions();
    }
    
    defs.add(orderClause, fmd);
 
  }

  private enum ClassMapping {
    INDEX;
  }
}
