package utils;

import com.examples.pubsub.streaming.options.PubSubToSpannerDBDataStreamingOptions;
import com.google.cloud.spanner.*;
import lombok.extern.slf4j.Slf4j;

import javax.xml.crypto.Data;

@Slf4j
public class DatabaseClientutils {

    public static DatabaseClient getSpannerDBClient(DatabaseId databaseId){
        try {
            SpannerOptions spannerOptions = SpannerOptions.newBuilder().build();
            Spanner spanner = spannerOptions.getService();
            return spanner.getDatabaseClient(databaseId);
        }catch(Exception e){
            log.error("failed to create a database client object. "+e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public static DatabaseAdminClient getSpannerDBAdminClient(DatabaseId databaseId){
        try {
            SpannerOptions spannerOptions = SpannerOptions.newBuilder().build();
            Spanner spanner = spannerOptions.getService();
            return spanner.getDatabaseAdminClient();
        }catch(Exception e){
            log.error("failed to create a database client object. "+e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

}
