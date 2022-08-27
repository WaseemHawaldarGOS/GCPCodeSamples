package com.examples.pubsub.streaming.model;


import lombok.*;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString(includeFieldNames=true)

@DefaultCoder(AvroCoder.class)
public class Employee {

    private long empid;
    private String empname;
    private String designation;

}
