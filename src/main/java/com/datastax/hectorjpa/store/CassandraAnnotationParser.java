/**
 * 
 */
package com.datastax.hectorjpa.store;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.DiscriminatorValue;
import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.Embedded;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.MappedSuperclass;

import org.apache.openjpa.conf.OpenJPAConfiguration;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
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
    mapping.put(Embedded.class, ClassMapping.EMBEDDED);
    mapping.put(ElementCollection.class, ClassMapping.EMBEDDEDCOLLECTION);
    mapping.put(MappedSuperclass.class, ClassMapping.MAPPEDSUPERCLASS);
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
        break;

      case MAPPEDSUPERCLASS:
        handleMappedSuperClass(cm, (MappedSuperclass) anno);
        break;
      }

    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.openjpa.persistence.AnnotationPersistenceMetaDataParser#
   * parseMemberMappingAnnotations(org.apache.openjpa.meta.FieldMetaData)
   */
  @Override
  protected void parseMemberMappingAnnotations(FieldMetaData fmd) {

    CassandraFieldMetaData cassField = (CassandraFieldMetaData) fmd;

    AnnotatedElement el = (AnnotatedElement) getRepository()
        .getMetaDataFactory().getDefaults().getBackingMember(fmd);

    ClassMapping mapped = null;

    for (Annotation annotation : el.getDeclaredAnnotations()) {
      mapped = mapping.get(annotation.annotationType());

      if (mapped == null) {
        continue;
      }

      switch (mapped) {
      case EMBEDDED:
        handleEmbedded(cassField);
        break;

      case EMBEDDEDCOLLECTION:
        handleEmbedded(cassField);
        break;
      }

    }

  }

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
  private void handleEmbeddable(CassandraClassMetaData cass,
      Embeddable embeddable) {
    if (!Serializable.class.isAssignableFrom(cass.getDescribedType())) {
      throw new MetaDataException(
          String
              .format(
                  "Embeddable classes must implement the interface '%s'.  The class '%s' does not",
                  Serializable.class.getName(), cass.getDescribedType()
                      .getName()));
    }

  }

  /**
   * Parse the cassandra index expression
   * 
   * @param fmd
   * @param index
   */
  private void handleMappedSuperClass(CassandraClassMetaData cass,
      MappedSuperclass superClass) {

    cass.setMappedSuperClass(true);
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

  /**
   * Parse the cassandra index expression
   * 
   * @param fmd
   * @param index
   */
  private void handleEmbedded(CassandraFieldMetaData fmd) {
    fmd.setSerializedEmbedded(true);
    // TODO TN do we actually need this check?
    // if (!Serializable.class.isAssignableFrom(fmd.getDeclaredType())) {
    // throw new MetaDataException(
    // String.format(
    // "Field '%s' was declared as embedded, but it is not serializable on class '%s'",
    // fmd.getName(), fmd.getDeclaringType()));
    // }

  }

  private enum ClassMapping {
    INDEX, INDEXES, DISCRIMINATOR, INHERITANCE, COLUMNFAMILY, EMBEDDABLE, EMBEDDED, EMBEDDEDCOLLECTION, MAPPEDSUPERCLASS;
  }
}
