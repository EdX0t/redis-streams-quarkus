package org.example.consumer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.redis.datasource.stream.StreamCommands;
import io.quarkus.redis.datasource.stream.StreamMessage;
import io.quarkus.redis.datasource.stream.XReadGroupArgs;
import org.jboss.logging.Logger;

import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

public class RedisWorker implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(RedisWorker.class);
    private static final SimpleDateFormat IDX_DF = new SimpleDateFormat("yyyyMMdd");
    private final String stream;
    private final String consumerGroup;
    private final String consumer;
    private final StreamCommands<String, String, String> streamCommands;
    private final ElasticsearchClient client;
    private final AtomicBoolean running;
    private final XReadGroupArgs xReadGroupArgs;
    private String lastId = "0";

    public RedisWorker(String stream,
                       String consumerGroup,
                       String consumer,
                       StreamCommands<String, String, String> streamCommands,
                       ElasticsearchClient client,
                       AtomicBoolean running) {
        this.stream = stream;
        this.consumerGroup = consumerGroup;
        this.consumer = consumer;
        this.streamCommands = streamCommands;
        this.client = client;
        this.running = running;

        this.xReadGroupArgs = new XReadGroupArgs();
        this.xReadGroupArgs.block(Duration.ofSeconds(2));
        this.xReadGroupArgs.count(10);
    }

    @Override
    public void run() {
        boolean processPending = true;
        while (running.get()) {
            try {
                lastId = processPending ? lastId : ">";
                List<StreamMessage<String, String, String>> messages =
                        streamCommands.xreadgroup(consumerGroup, consumer, stream, lastId, xReadGroupArgs);

                if (messages.isEmpty()) {
                    // Empty result when pending finished or current empty
                    // Set processPending false when lastId is 0
                    if (!lastId.equals(">"))
                        processPending = false;
                    Thread.sleep(1000);
                    continue;
                } else {
                    LOGGER.info((processPending ? "Pending " : "New ") + "size: " + messages.size());
                }

                processMessages(messages);

            } catch (Exception e) {
                LOGGER.error(e.getMessage());

                processPending = true;
                // reconnection loop
                try {
                    LOGGER.info("Waiting 5s before next retry");
                    int i = 0;
                    do {
                        Thread.sleep(1000);
                        i++;
                    } while (running.get() && i < 10);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    private void processMessages(List<StreamMessage<String, String, String>> messages) throws Exception {
        for (StreamMessage<String, String, String> message : messages) {
            ObjectMapper mapper = new ObjectMapper();

            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            df.setTimeZone(TimeZone.getTimeZone("UTC"));
            message.payload().put("timestamp", df.format(new Date()));
            String jsonResult = mapper.writeValueAsString(message.payload());
            LOGGER.info("id: " + message.id() + " key: " + message.key() + " payload: " + jsonResult);

            IndexRequest<String> request = IndexRequest.of(
                    b -> b.index("app_logs-" + IDX_DF.format(new Date()))
                            .withJson(new StringReader(jsonResult)));

            Result result = client.index(request).result();
            if (result.ordinal() != Result.Created.ordinal()) {
                throw new ConsumerException("Error indexing message. Result: " + result);
            }

            // Confirm that the message has been processed using XACK
            streamCommands.xack(stream, consumerGroup, message.id());
            lastId = message.id();
        }
    }
}
