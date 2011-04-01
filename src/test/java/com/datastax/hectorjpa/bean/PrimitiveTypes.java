/**
 * 
 */
package com.datastax.hectorjpa.bean;

import javax.persistence.Entity;

import org.apache.openjpa.persistence.Persistent;

import com.datastax.hectorjpa.annotation.ColumnFamily;

/**
 * Bean for testing all primitive types
 * @author Todd Nine
 *
 */
@Entity
@ColumnFamily("PrimitiveTypes")
public class PrimitiveTypes extends AbstractEntity {

  @Persistent
  private String string;
  
  @Persistent
  private char charValue;
  
  @Persistent
  private boolean boolValue;
  
  @Persistent
  private short shortVal;
  
  @Persistent
  private int intVal;
  
  @Persistent
  private double doubleVal;
  
  @Persistent
  private long longVal;
  
  @Persistent
  private float floatVal;
  
  @Persistent
  private TestEnum testEnum;

  /**
   * @return the string
   */
  public String getString() {
    return string;
  }

  /**
   * @param string the string to set
   */
  public void setString(String string) {
    this.string = string;
  }

  /**
   * @return the charValue
   */
  public char getCharValue() {
    return charValue;
  }

  /**
   * @param charValue the charValue to set
   */
  public void setCharValue(char charValue) {
    this.charValue = charValue;
  }

  /**
   * @return the boolValue
   */
  public boolean isBoolValue() {
    return boolValue;
  }

  /**
   * @param boolValue the boolValue to set
   */
  public void setBoolValue(boolean boolValue) {
    this.boolValue = boolValue;
  }

  /**
   * @return the shortVal
   */
  public short getShortVal() {
    return shortVal;
  }

  /**
   * @param shortVal the shortVal to set
   */
  public void setShortVal(short shortVal) {
    this.shortVal = shortVal;
  }

  /**
   * @return the intVal
   */
  public int getIntVal() {
    return intVal;
  }

  /**
   * @param intVal the intVal to set
   */
  public void setIntVal(int intVal) {
    this.intVal = intVal;
  }

  /**
   * @return the doubleVal
   */
  public double getDoubleVal() {
    return doubleVal;
  }

  /**
   * @param doubleVal the doubleVal to set
   */
  public void setDoubleVal(double doubleVal) {
    this.doubleVal = doubleVal;
  }

  /**
   * @return the longVal
   */
  public long getLongVal() {
    return longVal;
  }

  /**
   * @param longVal the longVal to set
   */
  public void setLongVal(long longVal) {
    this.longVal = longVal;
  }

  /**
   * @return the floatVal
   */
  public float getFloatVal() {
    return floatVal;
  }

  /**
   * @param floatVal the floatVal to set
   */
  public void setFloatVal(float floatVal) {
    this.floatVal = floatVal;
  }
  

  /**
   * @return the testEnum
   */
  public TestEnum getTestEnum() {
    return testEnum;
  }

  /**
   * @param testEnum the testEnum to set
   */
  public void setTestEnum(TestEnum testEnum) {
    this.testEnum = testEnum;
  }

  
  public enum TestEnum{
    ONE,
    TWO
  }
  
}
