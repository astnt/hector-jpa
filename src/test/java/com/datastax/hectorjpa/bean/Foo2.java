package com.datastax.hectorjpa.bean;

import com.datastax.hectorjpa.annotation.ColumnFamily;
import com.datastax.hectorjpa.annotation.Index;

import javax.persistence.*;

@Entity
@ColumnFamily("Foo2ColumnFamily")
@Index(fields = "other2")
@NamedQueries({ @NamedQuery(name = "searchRangeIncludeMinExcludeMaxWithLong",
        query = "select t from Foo2 as t where t.other2 >= :otherLow and t.other2 < :otherHigh")
})
public class Foo2 {
    @Id
    @GeneratedValue
    private Long id;
    private Long other2;

    public Foo2() {
    }

    public Foo2(Long other2) {
        this.other2 = other2;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getOther2() {
        return other2;
    }

    public void setOther2(Long other2) {
        this.other2 = other2;
    }
}
