/**
 * 
 */
package com.datastax.hectorjpa.query.iterator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

/**
 * Compiles one ore more ScanIterators into a List of dynamic columns that
 * represent a result set.
 * 
 * 
 * @author Todd Nine
 * 
 */
public class ResultCompiler {

  private List<DynamicComposite> results;

  private List<ScanIterator> iterators = new ArrayList<ScanIterator>();

  private Comparator<DynamicComposite> comparator;

  public ResultCompiler(Comparator<DynamicComposite> comparator) {
    this.comparator = comparator;
  }

  /**
   * Add a scan iterator to the result compilation
   * 
   * @param iterator
   */
  public void addScanIterator(ScanIterator iterator) {
    iterators.add(iterator);
  }

  /**
   * Compile the results into a single list
   */
  public void compile(int startIndex, int size) {
    if (iterators.size() == 1) {
      singleIterator(startIndex, size);
      return;
    }

    multipleIterators(startIndex, size);
  }
  
  /**
   * Get the results and return them
   * @return
   */
  public List<DynamicComposite> getResults(){
    return results;
  }

  /**
   * Used in OR expressions, where results need ordered and aggregated while
   * advancing
   * 
   * @param startIndex
   * @param size
   */
  private void multipleIterators(int startIndex, int size) {

    results = new ArrayList<DynamicComposite>();

    DynamicComposite max = null;
    DynamicComposite current = null;
    
    //pointer to the scan iterator that contains the element we want
    ScanIterator nextIterator = null;

    for (int i = 0; i < startIndex; i++) {

      for (ScanIterator currentItr : iterators) {

        current = currentItr.current();

        int result = comparator.compare(max, current);

        if (result > 0) {
          max = current;
          nextIterator = currentItr;
        }
       
      }
      
      //we now have our "next" element, which we discard since we're advancing, and our next iterator
      nextIterator.advance(1);
    }
    
    
    //now create our results
    results = new ArrayList<DynamicComposite>(size);
    
    for(int i = 0; i < size; i ++){
      
      for (ScanIterator currentItr : iterators) {

        current = currentItr.current();

        int result = comparator.compare(max, current);

        if (result > 0) {
          max = current;
          nextIterator = currentItr;
        }
      }
      
      results.add(max);
      nextIterator.advance(1);
      
    }
    
    
    

  }

  /**
   * Only 1 iterator is present, just advance it
   * 
   * @param startIndex
   * @param size
   */
  private void singleIterator(int startIndex, int size) {

    ScanIterator itr = iterators.get(0);

    int advanced = itr.advance(startIndex);

    // no results
    if (advanced < startIndex) {
      return;
    }

    itr.loadNext(size);

    results = new ArrayList<DynamicComposite>(itr.getColumns().size());
    
    for(HColumn<DynamicComposite, byte[]> col: itr.getColumns()){
      results.add(col.getName());
    }
    
    

  }

 

}
