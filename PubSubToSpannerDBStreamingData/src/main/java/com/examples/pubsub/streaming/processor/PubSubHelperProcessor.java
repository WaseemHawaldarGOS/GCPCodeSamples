package com.examples.pubsub.streaming.processor;

import com.examples.pubsub.streaming.options.PubSubToSpannerDBDataStreamingOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.values.PCollection;

public class PubSubHelperProcessor {

    private static final String NAME = "PubSub - Read Enriched Transaction";

    public PCollection<String> readMessage(Pipeline pipeline, PubSubToSpannerDBDataStreamingOptions options){
        PubsubIO.Read<String> pubsubIO = PubsubIO.readStrings().fromSubscription(options.getPubSubSubscription());

        return pipeline.apply(NAME, pubsubIO);
    }

}
