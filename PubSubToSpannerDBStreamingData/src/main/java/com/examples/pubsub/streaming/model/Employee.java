package com.examples.pubsub.streaming.model;


import lombok.*;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
@ToString
public class Employee implements Serializable {

    private long empid;
    private String empname;
    private String designation;

    public Employee(long empid, String empname, String designation) {
        this.empid = empid;
        this.empname = empname;
        this.designation = designation;
    }
}
