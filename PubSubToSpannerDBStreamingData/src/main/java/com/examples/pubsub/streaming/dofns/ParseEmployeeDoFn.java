package com.examples.pubsub.streaming.dofns;

import com.examples.pubsub.streaming.model.Employee;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseEmployeeDoFn extends DoFn<PubsubMessage, Employee> {
    private static final Logger LOG = LoggerFactory.getLogger(ParseEmployeeDoFn.class);

    private final String DELIMITER = ",";

    @ProcessElement
    public void processElement(ProcessContext c) {
        LOG.info("*****Inside ParseEmployeeDoFn processElement");
        LOG.info("Payload received is "+c.element().getPayload().toString());
        String[] employeeAttributes = c.element().getPayload().toString().split(DELIMITER);
        try {
            Long empId = Long.parseLong(employeeAttributes[0].trim());
            String empName = employeeAttributes[1].trim();
            String designation = employeeAttributes[2].trim();
            c.output(new Employee(empId, empName, designation));
        } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
            LOG.error("Exception occurred in ParseEmployeeDoFn. Error message: "+e.getMessage());
            e.printStackTrace();
        }
    }

}
