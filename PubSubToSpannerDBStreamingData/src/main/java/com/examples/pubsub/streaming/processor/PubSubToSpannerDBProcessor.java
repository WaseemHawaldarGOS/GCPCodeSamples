package com.examples.pubsub.streaming.processor;

import java.io.IOException;

import com.examples.pubsub.streaming.dofns.WriteEmployeeMutationDoFn;
import com.examples.pubsub.streaming.dofns.ParseEmployeeDoFn;
import com.examples.pubsub.streaming.model.Employee;
import com.examples.pubsub.streaming.options.PubSubToSpannerDBDataStreamingOptions;
import com.google.cloud.spanner.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class PubSubToSpannerDBProcessor {
    private static final long serialVersionUID = 1;

    private static final Logger LOG = LoggerFactory.getLogger(PubSubToSpannerDBProcessor.class);


    public static void run(Pipeline pipeline, PubSubToSpannerDBDataStreamingOptions options) throws IOException {

        LOG.info("Creating dialectView as Postgres");
        PCollectionView<Dialect> dialectView =
                pipeline.apply(Create.of(Dialect.POSTGRESQL)).apply(View.asSingleton());

        LOG.info("******Applying transformation on pubsub message and writing it to spanner DB started***");
        PCollection<String> pubSubMessages = new PubSubHelperProcessor().readMessage(pipeline, options);
        PCollection<Employee> employees = pubSubMessages.apply("Parse the employee data", ParDo.of(new ParseEmployeeDoFn()));
        employees.apply("Create and write Employee Mutation", ParDo.of(new WriteEmployeeMutationDoFn()));
        LOG.info("******Applying transformation on pubsub message and writing it to spanner DB completed***");
        pipeline.run();
    }

}