/**
 * 
 */
package com.datastax.hectorjpa.query.iterator;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.AbstractComposite.Component;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.hectorjpa.index.AbstractIndexOperation;

/**
 * Simple iterator that iterates over rows.
 * 
 * TODO get max size from settings. 
 * 
 * @author Todd Nine
 * 
 */
public class ScanIterator {

  private static final Logger logger = LoggerFactory.getLogger(ScanIterator.class);

  protected static int PAGE_SIZE = 500;

  private Keyspace keyspace;
  private DynamicComposite start;
  private DynamicComposite end;

  private byte[] indexName;

  private List<HColumn<DynamicComposite, byte[]>> columns;

  /**
   * value of -1 means the end of range has been reached
   */

  /**
   * The current page we're on
   */
  private int page;

  /**
   * The pointer to the current value in the columns in memory
   */
  private int index;

  public ScanIterator(Keyspace keyspace, DynamicComposite start,
      DynamicComposite end, byte[] indexName) {
    this.keyspace = keyspace;
    this.start = start;
    this.end = end;
    this.indexName = indexName;
    this.index = -1;
    this.page = 0;
  }

  /**
   * Read the the next "count" rows and advance the start pointer. Returns the
   * number of rows advanced, which may be less than the provided count if the
   * end of the range has been reached.
   * 
   * @param count
   */
  public int advance(int count) {
    if (count == 0) {
      return 0;
    }
    
    int endIndex = index+count;
    
    //advance in our currently loaded buffer
    if(columns != null && endIndex < columns.size()){
      index = endIndex;
      return count;
    }

    int startSize = columns == null ? 0: columns.size();
    
    int loaded = 0;

    // advance pages until we've advanced to the page we want
    while (page * PAGE_SIZE < count) {
      columns = nextPage(PAGE_SIZE, start);

      if (columns.size() > 0) {
        start = bumpComposite(columns.get(columns.size() - 1).getName());
      }

      loaded += columns.size();
   
      //we didn't have enough columns to load a full page, adjust the index and count
      if(columns.size() < PAGE_SIZE){
        
        //we couldn't load everything requested, set the index and return the loaded 
        if(loaded < count){
          index = columns.size()-1;
          return loaded+startSize-1;
        }
        
        break;
        
      }
      
      page++;
    }

    //we loaded everything
    index = count % PAGE_SIZE-1;
    return count;

  }

  /**
   * Get the current positition in the iterator.
   * 
   * @return
   */
  public DynamicComposite current() {
    if (index == -1) {
      return null;
    }

    return columns.get(index).getName();
  }

  /**
   * Load the next count records from cassandra and cache them in memory, does
   * not advance the start pointer
   * 
   * @param size
   * @return
   */
  public int loadNext(int count) {
    //special case where nothing is loaded
    if(columns == null){
      columns = nextPage(count, start);
      
      if(columns.size() > 0){
        index = 0;
        start = bumpComposite(columns.get(columns.size()-1).getName());
      }
      
      return columns.size();
    }
    
    int toLoad = count - (columns.size() - index-1);

    if (toLoad > 0) {
      columns.addAll(nextPage(toLoad,
          start));

    }

    return columns.size()-index-1;

  }

  /**
   * Get all columns that are currently loaded
   * 
   * @return
   */
  public List<HColumn<DynamicComposite, byte[]>> getColumns() {
    return columns;
  }

  /**
   * Increase the last comparator to GT_E to ensure we get the next page
   * 
   * @param composite
   * @return
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private DynamicComposite bumpComposite(DynamicComposite composite) {
    DynamicComposite newComposite = new DynamicComposite();

    int size = composite.getComponents().size();

    Component component = null;

    for (int i = 0; i < size - 1; i++) {

      component = composite.getComponent(i);

      newComposite.addComponent(component.getValue(),
          component.getSerializer(), component.getComparator(),
          component.getEquality());
    }

    component = composite.getComponent(size - 1);

    newComposite.addComponent(component.getValue(), component.getSerializer(),
        component.getComparator(), ComponentEquality.GREATER_THAN_EQUAL);

    return newComposite;

  }

  /**
   * Invoked to get the next page of results from cassandra
   */
  private List<HColumn<DynamicComposite, byte[]>> nextPage(int size,
      DynamicComposite startScan) {

    //we've advanced past the end, return an empty result set
    if(end != null && startScan.compareTo(end) > -1){
      return new ArrayList<HColumn<DynamicComposite, byte[]>>();
    }
    
    if(logger.isDebugEnabled()){
      
      String startHex = startScan == null ? "" : ByteBufferUtil.bytesToHex(startScan.serialize());
      String endHex = end == null? "" :  ByteBufferUtil.bytesToHex(end.serialize());
      
      logger.debug("Query start: {}, end: {}, size: {}", new Object[]{startHex, endHex, size});
    }
    
    SliceQuery<byte[], DynamicComposite, byte[]> sliceQuery = HFactory.createSliceQuery(keyspace, BytesArraySerializer.get(), DynamicCompositeSerializer.get(),
        BytesArraySerializer.get());
    QueryResult<ColumnSlice<DynamicComposite, byte[]>> result = null;

    sliceQuery.setRange(startScan, end, false, size);
    sliceQuery.setKey(indexName);
    sliceQuery.setColumnFamily(AbstractIndexOperation.CF_NAME);
    logger.debug("in executeQuery with sliceQuery {}", sliceQuery);
    result = sliceQuery.execute();
    
    List<HColumn<DynamicComposite, byte[]>> columns = result.get().getColumns();
    
    logger.debug("found {} results", columns.size());

    return columns;

  }

}
