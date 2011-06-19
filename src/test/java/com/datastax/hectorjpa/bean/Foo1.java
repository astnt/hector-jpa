package com.datastax.hectorjpa.bean;

import com.datastax.hectorjpa.annotation.ColumnFamily;
import com.datastax.hectorjpa.annotation.Index;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;

@Entity
@ColumnFamily("Foo1ColumnFamily")
@Index(fields = "other")
@NamedQueries({ @NamedQuery(name = "searchRangeIncludeMinExcludeMax",
        query = "select t from Foo1 as t where t.other >= :otherLow and t.other < :otherHigh"),
        @NamedQuery(name = "searchRangeIncludeMinIncludeMax",
                query = "select t from Foo1 as t where t.other >= :otherLow and t.other <= :otherHigh"),
        @NamedQuery(name = "searchRangeExcludeMinExcludeMax",
                query = "select t from Foo1 as t where t.other > :otherLow and t.other < :otherHigh"),
        @NamedQuery(name = "searchRangeExcludeMinIncludeMax",
                query = "select t from Foo1 as t where t.other > :otherLow and t.other <= :otherHigh") })
public class Foo1 {
    @Id
    @GeneratedValue
    private Long id;
    private Integer other;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Integer getOther() {
        return other;
    }

    public void setOther(Integer other) {
        this.other = other;
    }
}
