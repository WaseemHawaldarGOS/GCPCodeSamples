package com.examples.pubsub.streaming;

import java.io.IOException;
import org.apache.beam.examples.common.WriteOneFilePerWindow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

public class PubSubToGcsDataStreaming {

    /**
     * The below interface defines which options are mandatory for creating dataflow runner,
     * which needs to be passed while invoking PubSubToGcs.main
     * for instance, options.getInputTopic() is used in read Pubsub message stage etc.
     */
    public interface PubSubToGcsDataStreamingOptions extends PipelineOptions, StreamingOptions {
        @Description("The Cloud Pub/Sub topic to read from.")
        @Required
        String getInputTopic();

        void setInputTopic(String value);

        @Description("Output file's window size in number of minutes.")
        @Default.Integer(1)
        Integer getWindowSize();

        void setWindowSize(Integer value);

        @Description("Path of the output file (GCS bucket location) including its filename prefix.")
        @Required
        String getOutput();

        void setOutput(String value);
    }

    public static void main(String[] args) throws IOException {
        // The maximum number of shards when writing output.
        int numShards = 1;

        PubSubToGcsDataStreamingOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToGcsDataStreamingOptions.class);

        options.setStreaming(true);

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Read PubSub Streaming message which is placed by GCS cloud scheduler every 1 min", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
                .apply("Apply windowing on input stream data", Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))))
                .apply("Write the stream data as a separate files for every 1 min into GCS (GCS bucket location)", new WriteOneFilePerWindow(options.getOutput(), numShards));

        pipeline.run().waitUntilFinish();
    }
}