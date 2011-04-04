/**
 * 
 */
package com.datastax.hectorjpa.bean.tree;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

/**
 * @author Todd Nine
 *
 */
@Entity
@DiscriminatorValue("Nerd")
public class Nerd extends Techie {

}
