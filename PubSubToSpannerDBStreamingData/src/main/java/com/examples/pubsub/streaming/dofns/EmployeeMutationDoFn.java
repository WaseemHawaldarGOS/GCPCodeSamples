package com.examples.pubsub.streaming.dofns;

import com.examples.pubsub.streaming.model.Employee;
import com.google.cloud.spanner.Mutation;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmployeeMutationDoFn extends DoFn<Employee, Mutation> {
    private static final Logger LOG = LoggerFactory.getLogger(EmployeeMutationDoFn.class);

    private final String DELIMITER = ",";

    @ProcessElement
    public void processElement(ProcessContext c) {

        try {
            LOG.info("*****Inside EmployeeMutationDoFn processElement");
            Employee employee = c.element();
            LOG.info("Employee object received is " + employee.toString());
            c.output(Mutation.newInsertOrUpdateBuilder("employee")
                    .set("empid").to(employee.getEmpid())
                    .set("empname").to(employee.getEmpname())
                    .set("designation").to(employee.getDesignation())
                    .build());
        }catch(Exception e){
            LOG.error("Exception occurred in EmployeeMutationDoFn. Error message: "+e.getMessage());
            e.printStackTrace();
        }

    }
}
