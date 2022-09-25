package com.examples.pubsub.streaming;

import com.examples.pubsub.streaming.options.PubSubToSpannerDBDataStreamingOptions;
import com.examples.pubsub.streaming.processor.PubSubToSpannerDBProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

@Slf4j
public class Startup {

    public static void main(String[] args) {
        try {
            PubSubToSpannerDBDataStreamingOptions options = initializeOptions(args);
            options.setStreaming(true);
            Pipeline pipeline = Pipeline.create(options);
            new PubSubToSpannerDBProcessor().run(pipeline, options);
        }catch (Exception e){
            log.error("Exception occurred in Startup. "+e.getMessage());
            e.printStackTrace();
        }
    }

    private static PubSubToSpannerDBDataStreamingOptions initializeOptions(String[] args){
        return PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(PubSubToSpannerDBDataStreamingOptions.class);
    }
}
