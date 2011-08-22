/**
 * 
 */
package com.datastax.hectorjpa.bean;

import java.math.BigDecimal;
import java.util.Date;

import javax.persistence.Entity;

import org.apache.openjpa.persistence.Persistent;

import com.datastax.hectorjpa.annotation.ColumnFamily;
import com.datastax.hectorjpa.annotation.Index;
import com.datastax.hectorjpa.annotation.Indexes;

/**
 * @author Todd Nine
 * 
 */
@Entity
@ColumnFamily("InvoiceColumnFamily")
@Indexes({ 
    @Index(fields = "startDate", order = "amount desc"),
    @Index(fields = "amount", order = "startDate desc") 
})
public class Invoice extends AbstractEntity {

  @Persistent
  private Date startDate;

  @Persistent
  private Date endDate;

  @Persistent
  private BigDecimal amount;

  public Date getStartDate() {
    return startDate;
  }

  public void setStartDate(Date startDate) {
    this.startDate = startDate;
  }

  public Date getEndDate() {
    return endDate;
  }

  public void setEndDate(Date endDate) {
    this.endDate = endDate;
  }

  public BigDecimal getAmount() {
    return amount;
  }

  public void setAmount(BigDecimal amount) {
    this.amount = amount;
  }
}
