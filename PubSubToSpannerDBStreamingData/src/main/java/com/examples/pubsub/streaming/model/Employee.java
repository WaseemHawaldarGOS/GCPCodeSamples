package com.examples.pubsub.streaming.model;


import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString(includeFieldNames=true)

public class Employee {

    private long empid;
    private String empname;
    private String designation;

}
