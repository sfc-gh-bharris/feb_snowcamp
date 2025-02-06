package com.snowflake.streaming.app;

import java.io.*;
import java.util.Map;
import java.util.Properties;

import io.github.cdimascio.dotenv.Dotenv;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.streaming.OpenChannelRequest;

public class App {
    private static final Logger LOGGER = LoggerFactory.getLogger(App.class.getName());

    public static void main(String[] args) throws Exception {
        Dotenv dotenv = Dotenv.configure().load();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        Properties props = new Properties();
        props.put("user", dotenv.get("SNOWFLAKE_USER"));
        props.put("url", "https://" + dotenv.get("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com:443");
        props.put("private_key", dotenv.get("PRIVATE_KEY"));
        props.put("role", "INGEST");

        try (SnowflakeStreamingIngestClient client = SnowflakeStreamingIngestClientFactory.builder("MY_CLIENT")
                .setProperties(props).build()) {
            OpenChannelRequest request1 = OpenChannelRequest.builder("MY_CHANNEL")
                    .setDBName("INGEST")
                    .setSchemaName("INGEST")
                    .setTableName("LIFT_TICKETS_JAVA_STREAMING")
                    .setOnErrorOption(
                            OpenChannelRequest.OnErrorOption.ABORT)
                    .build();

            SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);
            String line = br.readLine();
            int val = 0;
            while (line != null && line.length() > 0) {
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object> map = mapper.readValue(line, Map.class);

                InsertValidationResponse response = channel1.insertRow(map, String.valueOf(val));
                if (response.hasErrors()) {
                    System.out.println(response.getInsertErrors().get(0).getException());
                }

                line = br.readLine();
                val++;
            }
            LOGGER.info("Ingest complete");
            channel1.close().get();
        }
    }
}