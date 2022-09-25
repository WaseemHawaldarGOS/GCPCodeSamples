package com.examples.pubsub.streaming.dofns;

import com.examples.pubsub.streaming.options.PubSubToSpannerDBDataStreamingOptions;
import com.examples.pubsub.streaming.processor.PubSubToSpannerDBProcessor;
import com.examples.pubsub.streaming.model.Employee;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ParseEmployeeDoFn extends DoFn<String, Employee>{
    private static final long serialVersionUID = 1;

    private static final Logger LOG = LoggerFactory.getLogger(ParseEmployeeDoFn.class);

    private final String DELIMITER = ",";

    public ParseEmployeeDoFn() {
    }

    @ProcessElement
    public void processElement(ProcessContext c) {



        LOG.info("*****Inside ParseEmployeeDoFn processElement");
        LOG.info("Payload received is "+c.element().toString());
        String[] employeeAttributes = c.element().toString().split(DELIMITER);
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
