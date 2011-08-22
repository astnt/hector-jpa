/**
 * 
 */
package com.datastax.hectorjpa.index;

import static com.datastax.hectorjpa.serializer.CompositeUtils.newComposite;
import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.openjpa.kernel.OpenJPAStateManager;

import com.datastax.hectorjpa.query.IndexQuery;
import com.datastax.hectorjpa.query.field.FieldExpression;
import com.datastax.hectorjpa.query.iterator.ScanBuffer;
import com.datastax.hectorjpa.service.IndexAudit;
import com.datastax.hectorjpa.service.IndexQueue;
import com.datastax.hectorjpa.store.CassandraClassMetaData;

/**
 * Class to perform all operations for secondary indexing on an instance in the
 * statemanager.  
 * 
 * @author Todd Nine
 * 
 */
public class SubclassIndexOperation extends AbstractIndexOperation {
 /**
   * String array of all subclass discriminator values
   */
  private String[] subClasses;
  //the discirminator value for the class that owns this instance.  I.E. same from class metaData
  
  private String discriminatorValue;

  public SubclassIndexOperation(CassandraClassMetaData metaData,
      IndexDefinition indexDef) {
    super(metaData, indexDef);

    subClasses = metaData.getSuperClassDiscriminators();
    
    this.discriminatorValue = metaData.getDiscriminatorColumn();

  }

  /**
   * Write the index definition
   * 
   * @param stateManager
   *          The objects state manager
   * @param mutator
   *          The mutator to write to
   * @param clock
   *          the clock value to use
   */
  public void writeIndex(OpenJPAStateManager stateManager,
      Mutator<byte[]> mutator, long clock, IndexQueue queue) {

    DynamicComposite searchComposite = null;
    DynamicComposite tombstoneComposite = null;
    DynamicComposite idAudit = null;

    // loop through all added objects and create the writes for them.
    // create our composite of the format of id+order*

    for (int i = 0; i < subClasses.length; i++) {

      searchComposite = newComposite();

      searchComposite.addComponent(subClasses[i], stringSerializer);

      // create our composite of the format order*+id
      
      tombstoneComposite = newComposite();
      
      tombstoneComposite.addComponent(1, subClasses[i], stringSerializer, stringSerializer.getComparatorType().getTypeName(), ComponentEquality.EQUAL);
      
      idAudit = newComposite();
      
      idAudit.addComponent(1, subClasses[i], stringSerializer, stringSerializer.getComparatorType().getTypeName(), ComponentEquality.EQUAL);

      constructComposites(searchComposite, tombstoneComposite, idAudit, stateManager);

      
      mutator.addInsertion(indexName, CF_NAME,
          new HColumnImpl<DynamicComposite, byte[]>(searchComposite, HOLDER,
              clock, compositeSerializer, bytesSerializer));
      
      mutator.addInsertion(reverseIndexName, CF_NAME,
              new HColumnImpl<DynamicComposite, byte[]>(tombstoneComposite, HOLDER,
                  clock, compositeSerializer, bytesSerializer));
      
      
      queue.addAudit(new IndexAudit(indexName, reverseIndexName, idAudit, clock, CF_NAME, true));


    }

  }

  /**
   * Scan the given index query and add the results to the provided set. The set
   * comparator of the dynamic columns are compared via a tree comparator
   * 
   * @param query
   * @return 
   */
  public ScanBuffer scanIndex(IndexQuery query, Keyspace keyspace) {
    
   
    DynamicComposite startScan = newComposite();
    DynamicComposite endScan = newComposite();

    //add the discriminator value so we're querying for the specified class
    //and it's children
     
    startScan.addComponent(discriminatorValue, stringSerializer);
    endScan.addComponent(discriminatorValue, stringSerializer);

    int length = fields.length;
    
    int last = length -1;
    int componentIndex = 1;

    FieldExpression exp = null;
    
    for (int i = 0; i < last; i++, componentIndex++) {

      exp = query.getExpression(this.fields[i].getMetaData());

      this.fields[i].addToComposite(startScan, componentIndex, exp.getStart(),
          ComponentEquality.EQUAL);
      this.fields[i].addToComposite(endScan, componentIndex, exp.getEnd(),
    		  ComponentEquality.EQUAL);
    }
    
    exp = query.getExpression(this.fields[last].getMetaData());
    
    this.fields[last].addToComposite(startScan, componentIndex, exp.getStart(), exp.getStartEquality());
    this.fields[last].addToComposite(endScan, componentIndex, exp.getEnd(), exp.getEndEquality());
    
    
    return new ScanBuffer(keyspace, startScan, endScan, indexName);

  }
  

  /**
   * @return the indexDefinition
   */
  public IndexDefinition getIndexDefinition() {
    return indexDefinition;
  }



}
