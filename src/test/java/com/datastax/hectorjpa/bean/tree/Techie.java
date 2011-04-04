/**
 * 
 */
package com.datastax.hectorjpa.bean.tree;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

import org.apache.openjpa.persistence.Persistent;

import com.datastax.hectorjpa.annotation.ColumnFamily;
import com.datastax.hectorjpa.annotation.Index;
import com.datastax.hectorjpa.bean.AbstractEntity;

/**
 * @author Todd Nine
 *
 */
@Entity
@ColumnFamily("TechieColumnFamily")
@DiscriminatorValue("Techie")
@Index(fields="name")
public class Techie extends AbstractEntity {

	@Persistent
	public String name;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	
	
}
