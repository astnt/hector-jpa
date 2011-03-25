/**
 * 
 */
package com.datastax.hectorjpa.store;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;

import org.apache.openjpa.conf.OpenJPAConfiguration;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.persistence.AnnotationPersistenceMetaDataParser;
import org.apache.openjpa.util.MetaDataException;

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
		mapping.put(DiscriminatorValue.class, ClassMapping.DISCRIMINATOR);
		mapping.put(Inheritance.class, ClassMapping.INHERITANCE);
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

			case INDEX:
				handleIndex(cassField, (Index) annotation);
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
	private void handleIndex(CassandraFieldMetaData fmd, Index index) {

		String orderClause = index.value();

		IndexDefinitions defs = (IndexDefinitions) fmd
				.getObjectExtension(IndexDefinitions.EXT_NAME);

		if (defs == null) {
			defs = new IndexDefinitions();
		}

		defs.add(orderClause, fmd);

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
			throw new MetaDataException(
					"Only single table inheritance is supported");
		}

	}

	private enum ClassMapping {
		INDEX, DISCRIMINATOR, INHERITANCE;
	}
}
