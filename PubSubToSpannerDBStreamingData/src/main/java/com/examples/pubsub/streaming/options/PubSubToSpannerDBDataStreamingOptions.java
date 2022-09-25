package com.examples.pubsub.streaming.options;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.*;

/**
 * The below interface defines which options are mandatory for creating dataflow runner,
 * which needs to be passed while invoking PubSubToGcs.main
 * for instance, options.getInputTopic() is used in read Pubsub message stage etc.
 */
public interface PubSubToSpannerDBDataStreamingOptions extends PubsubOptions, StreamingOptions {
    @Description("The Cloud Pub/Sub subscription to read from")
    @Validation.Required
    String getPubSubSubscription();

    void setPubSubSubscription(String value);

    @Description("The Cloud Pub/Sub host to read from")
    String getPubSubHost();

    void setPubSubHost(String value);

    @Description("The Spanner host to write to")
    String getSpannerHost();

    void setSpannerHost(String value);

    @Description("Spanner Instance ID.")
    @Validation.Required
    String getInstanceID();

    void setInstanceID(String value);

    @Description("Spanner Database ID")
    @Validation.Required
    String getDatabaseID();

    void setDatabaseID(String value);

    //commenting it out below as it comes from parent class which is GcpOptions
    /*@Description("The Project ID")
    @Validation.Required
    @Default
    String getProject();

    void setProject(String value);*/



}
