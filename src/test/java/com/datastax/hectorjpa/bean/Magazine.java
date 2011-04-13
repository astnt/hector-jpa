/**
 * 
 */
package com.datastax.hectorjpa.bean;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;

import org.apache.openjpa.persistence.Persistent;

import com.datastax.hectorjpa.annotation.ColumnFamily;

/**
 * Simple class that tests composite keys
 * 
 * @author Todd Nine
 * 
 */
@ColumnFamily("MagazineColumnFamily")
@Entity
@IdClass(Magazine.MagazineId.class)
public class Magazine{

  @Id
  private String isbn;
  
  @Id
  private String title;  
  
  @Persistent
  private long copiesSold;

  
 
  /**
   * @return the isbn
   */
  public String getIsbn() {
    return isbn;
  }



  /**
   * @param isbn the isbn to set
   */
  public void setIsbn(String isbn) {
    this.isbn = isbn;
  }



  /**
   * @return the title
   */
  public String getTitle() {
    return title;
  }



  /**
   * @param title the title to set
   */
  public void setTitle(String title) {
    this.title = title;
  }



  /**
   * @return the copiesSold
   */
  public long getCopiesSold() {
    return copiesSold;
  }



  /**
   * @param copiesSold the copiesSold to set
   */
  public void setCopiesSold(long copiesSold) {
    this.copiesSold = copiesSold;
  }

  


  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((isbn == null) ? 0 : isbn.hashCode());
    result = prime * result + ((title == null) ? 0 : title.hashCode());
    return result;
  }



  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof Magazine))
      return false;
    Magazine other = (Magazine) obj;
    if (isbn == null) {
      if (other.isbn != null)
        return false;
    } else if (!isbn.equals(other.isbn))
      return false;
    if (title == null) {
      if (other.title != null)
        return false;
    } else if (!title.equals(other.title))
      return false;
    return true;
  }




  public static class MagazineId {
    
    private String isbn;
    
    private String title;

    
    public MagazineId() {
      super();
    }


    public MagazineId(String isbn, String title) {
      super();
      this.isbn = isbn;
      this.title = title;
    }


    /**
     * @return the isbn
     */
    public String getIsbn() {
      return isbn;
    }


    /**
     * @return the title
     */
    public String getTitle() {
      return title;
    }


    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((isbn == null) ? 0 : isbn.hashCode());
      result = prime * result + ((title == null) ? 0 : title.hashCode());
      return result;
    }


    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (!(obj instanceof MagazineId))
        return false;
      MagazineId other = (MagazineId) obj;
      if (isbn == null) {
        if (other.isbn != null)
          return false;
      } else if (!isbn.equals(other.isbn))
        return false;
      if (title == null) {
        if (other.title != null)
          return false;
      } else if (!title.equals(other.title))
        return false;
      return true;
    }  
    
    
  }

}
