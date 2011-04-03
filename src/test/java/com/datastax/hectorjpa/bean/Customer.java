/**
 * 
 */
package com.datastax.hectorjpa.bean;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.ElementCollection;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;

import org.apache.openjpa.persistence.Persistent;

import com.datastax.hectorjpa.annotation.ColumnFamily;

/**
 * @author Todd Nine
 * 
 */
@Entity
@ColumnFamily("CustomerColumnFamily")
public class Customer extends AbstractEntity {

	@Persistent
	private String name;

	@Embedded
	private Phone phoneNumber;

	@ElementCollection
	private List<Phone> otherPhones;

	@Persistent
	private String email;

	@ManyToOne
	private Store store;

	@OneToMany(cascade = CascadeType.ALL, orphanRemoval = true, mappedBy = "customer")
	private List<Sale> sales;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Phone getPhoneNumber() {
		return phoneNumber;
	}

	/**
	 * @param phoneNumber
	 *            the phoneNumber to set
	 */
	public void setPhoneNumber(Phone phoneNumber) {
		this.phoneNumber = phoneNumber;
	}

	public List<Phone> getOtherPhones() {
		return otherPhones;
	}
	
	public void addOtherPhone(Phone phone){
		if(otherPhones == null){
			otherPhones = new ArrayList<Phone>();
		}
		
		otherPhones.add(phone);
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public Store getStore() {
		return store;
	}

	public void setStore(Store store) {
		this.store = store;
	}

	/**
	 * @return the sales
	 */
	public List<Sale> getSales() {

		return sales;
	}

	/**
	 * Add the sale to the customer
	 * 
	 * @param sale
	 */
	public void addSale(Sale sale) {
		if (sales == null) {
			this.sales = new ArrayList<Sale>();
		}

		this.sales.add(sale);
	}

}
