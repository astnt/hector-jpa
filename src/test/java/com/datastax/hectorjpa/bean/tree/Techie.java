/**
 * 
 */
package com.datastax.hectorjpa.bean.tree;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.DiscriminatorValue;
import javax.persistence.ElementCollection;
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
	
	@ElementCollection
	public Set<Role> roles;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public void addRole(Role role){
	  if(roles == null){
	    roles = new HashSet<Role>();
	  }
	  
	  roles.add(role);
	}
	
	public Set<Role> getRoles(){
	  return roles;
	}
	
	
	
	
	
}
