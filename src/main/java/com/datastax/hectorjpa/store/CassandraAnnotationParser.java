/**
 * 
 */
package com.datastax.hectorjpa.store;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Embeddable;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;

import org.apache.openjpa.conf.OpenJPAConfiguration;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.persistence.AnnotationPersistenceMetaDataParser;
import org.apache.openjpa.util.MetaDataException;

import com.datastax.hectorjpa.annotation.ColumnFamily;
import com.datastax.hectorjpa.annotation.Index;
import com.datastax.hectorjpa.annotation.Indexes;
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
    mapping.put(Indexes.class, ClassMapping.INDEXES);
    mapping.put(DiscriminatorValue.class, ClassMapping.DISCRIMINATOR);
    mapping.put(Inheritance.class, ClassMapping.INHERITANCE);
    mapping.put(ColumnFamily.class, ClassMapping.COLUMNFAMILY);
    mapping.put(Embeddable.class, ClassMapping.EMBEDDABLE);
  }

  public CassandraAnnotationParser(OpenJPAConfiguration conf) {
    super(conf);
  }

  @Override
  protected void parseClassMappingAnnotations(ClassMetaData meta) {

    CassandraClassMetaData cm = (CassandraClassMetaData) meta;
    Class<?> cls = cm.getDescribedType();

    ClassMapping mapped = null;

    for (Annotation anno : cls.getDeclaredAnnotations()) {
      mapped = mapping.get(anno.annotationType());
      if (mapped == null) {
        continue;
      }

      switch (mapped) {

      case DISCRIMINATOR:
        handleDiscrimiantor(cm, (DiscriminatorValue) anno);
        break;

      case INHERITANCE:
        handleInheritance(cm, (Inheritance) anno);
        break;

      case COLUMNFAMILY:
        handleColumnFamily(cm, (ColumnFamily) anno);
        break;

      case INDEX:
        handleIndex(cm, (Index) anno);
        break;

      case INDEXES:
        handleIndexes(cm, (Indexes) anno);
        break;
        
      case EMBEDDABLE:
        handleEmbeddable(cm, (Embeddable) anno);

      }

    }
  }

  // /*
  // * (non-Javadoc)
  // *
  // * @see org.apache.openjpa.persistence.AnnotationPersistenceMetaDataParser#
  // * parseMemberMappingAnnotations(org.apache.openjpa.meta.FieldMetaData)
  // */
  // @Override
  // protected void parseMemberMappingAnnotations(FieldMetaData fmd) {
  //
  // CassandraFieldMetaData cassField = (CassandraFieldMetaData) fmd;
  //
  // AnnotatedElement el = (AnnotatedElement) getRepository()
  // .getMetaDataFactory().getDefaults().getBackingMember(fmd);
  //
  // ClassMapping mapped = null;
  //
  // for (Annotation annotation : el.getDeclaredAnnotations()) {
  // mapped = mapping.get(annotation.annotationType());
  //
  // if (mapped == null) {
  // continue;
  // }
  //
  // switch (mapped) {
  //
  // }
  //
  //
  // }
  //
  // }

  /**
   * Parse the cassandra index expression
   * 
   * @param fmd
   * @param index
   */
  private void handleIndex(CassandraClassMetaData cass, Index index) {

    IndexDefinitions defs = cass.getIndexDefinitions();

    if (defs == null) {
      defs = new IndexDefinitions();
      cass.setIndexDefinitions(defs);
    }

    defs.add(index.fields(), index.order(), cass);

  }
  
  /**
   * Parse the cassandra index expression
   * 
   * @param fmd
   * @param index
   */
  private void handleEmbeddable(CassandraClassMetaData cass, Embeddable embeddable) {
    if(!Serializable.class.isAssignableFrom(cass.getDescribedType())){
      throw new MetaDataException(String.format("Embeddable classes must implement the interface '%s'.  The class '%s' does not", Serializable.class.getName(), cass.getDescribedType().getName()));
    }

  }

  /**
   * Parse the cassandra index expression
   * 
   * @param fmd
   * @param index
   */
  private void handleIndexes(CassandraClassMetaData cass, Indexes index) {

    for (Index current : index.value()) {
      handleIndex(cass, current);
    }

  }

  /**
   * Parse the cassandra discriminator
   * 
   * @param fmd
   * @param index
   */
  private void handleDiscrimiantor(CassandraClassMetaData cass,
      DiscriminatorValue discriminator) {
    cass.setDiscriminatorColumn(discriminator.value());
  }

  /**
   * Parse the cassandra index expression
   * 
   * @param fmd
   * @param index
   */
  private void handleInheritance(CassandraClassMetaData cass,
      Inheritance inheritance) {

    if (inheritance.strategy() != InheritanceType.SINGLE_TABLE) {
      throw new MetaDataException("Only single table inheritance is supported");
    }

  }

  /**
   * 
   * @param cass
   * @param inheritance
   */
  private void handleColumnFamily(CassandraClassMetaData cass, ColumnFamily cf) {

    String name = cf.value();

    if (name == null) {
      throw new MetaDataException(
          "You must specify a name in the column family");
    }

    cass.setColumnFamily(name);

  }

  private enum ClassMapping {
    INDEX, INDEXES, DISCRIMINATOR, INHERITANCE, COLUMNFAMILY, EMBEDDABLE;
  }
}
