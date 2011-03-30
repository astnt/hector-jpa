/**
 * 
 */
package com.datastax.hectorjpa.meta;

import static com.datastax.hectorjpa.serializer.CompositeUtils.newComposite;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.openjpa.kernel.OpenJPAStateManager;

import com.datastax.hectorjpa.index.IndexDefinition;
import com.datastax.hectorjpa.query.FieldExpression;
import com.datastax.hectorjpa.query.IndexQuery;
import com.datastax.hectorjpa.store.CassandraClassMetaData;

/**
 * Class to perform all operations for secondary indexing on an instance in the
 * statemanager
 * 
 * @author Todd Nine
 * 
 */
public class SubclassIndexOperation extends AbstractIndexOperation {

  /**
   * String array of all subclass discriminator values
   */
  private String[] subClasses;

  public SubclassIndexOperation(CassandraClassMetaData metaData,
      IndexDefinition indexDef) {
    super(metaData, indexDef);

    subClasses = getSubclasses(metaData);

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
      Mutator<byte[]> mutator, long clock) {

    DynamicComposite newComposite = null;
    DynamicComposite oldComposite = null;

    // loop through all added objects and create the writes for them.
    // create our composite of the format of id+order*

    for (int i = 0; i < subClasses.length; i++) {

      newComposite = newComposite();

      newComposite.addComponent(subClasses[i], stringSerializer);

      // create our composite of the format order*+id
      oldComposite = newComposite();

      oldComposite.addComponent(subClasses[i], stringSerializer);

      boolean changed = constructComposites(newComposite, oldComposite,
          stateManager);

      mutator.addInsertion(indexName, CF_NAME,
          new HColumnImpl<DynamicComposite, byte[]>(newComposite, HOLDER,
              clock, compositeSerializer, bytesSerializer));

      // value has changed since we loaded. Remove the old value
      if (changed) {

        // add it to our old value
        mutator.addDeletion(indexName, CF_NAME, oldComposite,
            compositeSerializer, clock);

      }
    }

  }

  /**
   * Scan the given index query and add the results to the provided set. The set
   * comparator of the dynamic columns are compared via a tree comparator
   * 
   * @param query
   */
  public void scanIndex(IndexQuery query, Set<DynamicComposite> results,
      Keyspace keyspace) {
    
   
    DynamicComposite startScan = newComposite();
    DynamicComposite endScan = newComposite();

    //add the discriminator value so we're querying for the specified class
    //and it's children
    String discriminator = indexDefinition.getMetaData().getDiscriminatorColumn();
    
    startScan.addComponent(discriminator, stringSerializer);
    endScan.addComponent(discriminator, stringSerializer);
    
    int index = 0;

    // add all fields
    for (FieldExpression exp : query.getExpressions()) {
      index = this.fieldIndexes.get(exp.getField().getName());
      //inclusive adds the byte 1 to the end of the field.  If it's inclusive on the start we want to set to false
      //so that this byte is 0
      this.fields[index].addToComposite(startScan, index+1,  exp.getStart(), !exp.isStartInclusive());
      this.fields[index].addToComposite(endScan, index+1, exp.getEnd(), exp.isEndInclusive());
    }

    // now query the values
    // get our slice range
    super.executeQuery(startScan, endScan, results, keyspace);

  }

  /**
   * Get all discriminator values including the current class. If one is not
   * present an null array is returned
   * 
   * @param cmd
   * @return
   */
  private String[] getSubclasses(CassandraClassMetaData cmd) {
    if (cmd.getDiscriminatorColumn() == null) {
      return null;
    }

    // TODO TN use a depth weight algorithm to determine the maximum common
    // ancestor for all fields in the index. For now this naive approach
    // just uses discriminator values from all parent classes.

    List<String> subclasses = new ArrayList<String>();

    CassandraClassMetaData current = cmd;

    do {
      // TODO TN, should probably throw a metadata exception here
      if (current.getDiscriminatorColumn() == null) {
        break;
      }

      subclasses.add(current.getDiscriminatorColumn());
      current = (CassandraClassMetaData) current.getPCSuperclassMetaData();
    } while (current != null);

    String[] subArray = new String[subclasses.size()];

    subclasses.toArray(subArray);

    return subArray;

  }

  /**
   * @return the indexDefinition
   */
  public IndexDefinition getIndexDefinition() {
    return indexDefinition;
  }

}
