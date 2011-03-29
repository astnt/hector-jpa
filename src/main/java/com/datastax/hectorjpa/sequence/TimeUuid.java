package com.datastax.hectorjpa.sequence;

import java.util.LinkedList;
import java.util.Queue;

import org.apache.openjpa.kernel.Seq;
import org.apache.openjpa.kernel.StoreContext;
import org.apache.openjpa.meta.ClassMetaData;

import com.eaio.uuid.UUID;

/**
 * Class to generate time uuids in batch
 * 
 * 
 * @author Todd Nine
 *
 */
public class TimeUuid implements Seq{

	/**
	 * The name of this generator
	 */
	public static final String NAME = "timeuuid";
	
	private Queue<UUID> ids = new LinkedList<UUID>();
	

	@Override
	public void setType(int type) {
	
		
	}

	@Override
	public Object next(StoreContext ctx, ClassMetaData cls) {
		//TODO TN figure out why open JPA is not calling allocate 
		if(ids.size() == 0){
			allocate(100, ctx, cls);
		}
		
		return ids.remove();
	}

	@Override
	public Object current(StoreContext ctx, ClassMetaData cls) {
		return null;
	}

	@Override
	public void allocate(int additional, StoreContext ctx, ClassMetaData cls) {
		for(int i = 0; i < additional; i ++){
			ids.add(new UUID());
		}
		
	}

	@Override
	public void close() {
		//do nothing
		
	}

}
