package com.examples.pubsub.streaming.dofns;

import com.examples.pubsub.streaming.model.Employee;
import com.examples.pubsub.streaming.options.PubSubToSpannerDBDataStreamingOptions;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.*;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.DatabaseClientutils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class WriteEmployeeMutationDoFn extends DoFn<Employee, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(WriteEmployeeMutationDoFn.class);
    List<Mutation> employeeMutations = new ArrayList<>();
    PubSubToSpannerDBDataStreamingOptions options;

    @Setup
    public void setup(PipelineOptions po){
        options = po.as(PubSubToSpannerDBDataStreamingOptions.class);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

        try {
            DatabaseId databaseId = DatabaseId.of(options.getProject(), options.getInstanceID(), options.getDatabaseID());
            DatabaseClient databaseClient = DatabaseClientutils.getSpannerDBClient(databaseId);
            //DatabaseAdminClient databaseAdminClient = DatabaseClientutils.getSpannerDBAdminClient(databaseId);


            /*
            below code creates employee table
             */
            //LOG.info("Creating employee table in spanner emulator");
            //createTableUsingDdl(databaseAdminClient, databaseId);

            LOG.info("*****Inside WriteEmployeeMutationDoFn processElement");
            Employee employee = c.element();
            LOG.info("Employee object received is " + employee.toString());
            LOG.info("Creating an employee mutation");
            employeeMutations.add(Mutation.newInsertOrUpdateBuilder("employee")
                    .set("empid").to(employee.getEmpid())
                    .set("empname").to(employee.getEmpname()+"_updated")
                    .set("designation").to(employee.getDesignation()+"_updated")
                    .build());
            LOG.info("Persisting an employee mutation");

            databaseClient.write(employeeMutations);
        }catch(Exception e){
            LOG.error("Exception occurred in WriteEmployeeMutationDoFn. Error message: "+e.getMessage());
            e.printStackTrace();
        }

    }

    static void createTableUsingDdl(DatabaseAdminClient dbAdminClient, DatabaseId id) {
        // public Database(DatabaseId id, DatabaseInfo.State state, DatabaseAdminClient dbClient)
        Database database = new Database(id, DatabaseInfo.State.READY, dbAdminClient);
        dbAdminClient.createDatabase(database, null);
        OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
                dbAdminClient.updateDatabaseDdl(
                        id.getInstanceId().getInstance(),
                        id.getDatabase(),
                        Arrays.asList(
                                "CREATE TABLE employee ("
                                        + "  empid   bigint NOT NULL,"
                                        + "  empname  character varying(1024)"
                                        + "  PRIMARY KEY (empid)"
                                        + ")"),
                        null);
        try {
            // Initiate the request which returns an OperationFuture.
            op.get();
            System.out.println("Successfully Created employee table in database: [" + id + "]");
        } catch (ExecutionException e) {
            // If the operation failed during execution, expose the cause.
            throw (SpannerException) e.getCause();
        } catch (InterruptedException e) {
            // Throw when a thread is waiting, sleeping, or otherwise occupied,
            // and the thread is interrupted, either before or during the activity.
            throw SpannerExceptionFactory.propagateInterrupt(e);
        }
    }
}
