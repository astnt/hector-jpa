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
@DiscriminatorValue("Geek")
public class Geek extends Techie {

}
