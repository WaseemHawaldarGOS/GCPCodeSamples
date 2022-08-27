package com.examples.pubsub.streaming;

import java.io.IOException;

import com.examples.pubsub.streaming.dofns.EmployeeMutationDoFn;
import com.examples.pubsub.streaming.dofns.ParseEmployeeDoFn;
import com.google.cloud.spanner.Dialect;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubToSpannerDBDataStreaming {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubToSpannerDBDataStreaming.class);

    /**
     * The below interface defines which options are mandatory for creating dataflow runner,
     * which needs to be passed while invoking PubSubToGcs.main
     * for instance, options.getInputTopic() is used in read Pubsub message stage etc.
     */
    public interface PubSubToSpannerDBDataStreamingOptions extends PipelineOptions, StreamingOptions {
        @Description("The Cloud Pub/Sub topic to read from.")
        @Required
        String getInputTopic();

        void setInputTopic(String value);

        @Description("Spanner Instance ID.")
        @Required
        String getInstanceID();

        void setInstanceID(String value);

        @Description("Spanner Database ID")
        @Required
        String getDatabaseID();

        void setDatabaseID(String value);

    }

    public static void main(String[] args) throws IOException {
        // The maximum number of shards when writing output.
        int numShards = 1;

        LOG.info("Inside PubSubToSpannerDBDataStreaming main method, input arguments received are : ");
        for(String arg : args){
            LOG.info("project = "+args[0]);
            LOG.info("region = "+args[1]);
            LOG.info("inputTopic = "+args[2]);
            LOG.info("runner = "+args[3]);
            LOG.info("instanceId = "+args[4]);
            LOG.info("databaseId = "+args[5]);
        }

        LOG.info("Creating options for dataflow runner pipeline");
        PubSubToSpannerDBDataStreamingOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToSpannerDBDataStreamingOptions.class);

        LOG.info("setting streaming parameter as true");
        options.setStreaming(true);

        LOG.info("Creating pipeline with options");
        Pipeline pipeline = Pipeline.create(options);

        LOG.info("Creating dialectView as Postgres");
        PCollectionView<Dialect> dialectView =
                pipeline.apply(Create.of(Dialect.POSTGRESQL)).apply(View.asSingleton());

        LOG.info("******Applying transformation on pubsub message and writing it to spanner BD started***");
        pipeline.apply("Read PubSub Messages", PubsubIO.readMessagesWithAttributesAndMessageId().fromTopic(options.getInputTopic()))
                .apply("Parse the employee data", ParDo.of(new ParseEmployeeDoFn()))
                .apply("Create Employee Mutation", ParDo.of(new EmployeeMutationDoFn()))
                // Write mutations to Spanner
                .apply("Write employee mutation to spanner DB", SpannerIO.write()
                        .withInstanceId(options.getInstanceID())
                        .withDatabaseId(options.getDatabaseID())
                        .withDialectView(dialectView));

        LOG.info("******Applying transformation on pubsub message and writing it to spanner BD completed***");
        pipeline.run().waitUntilFinish();
    }

}