package org.example.consumer;

import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.stream.StreamCommands;
import io.quarkus.redis.datasource.stream.StreamMessage;
import io.quarkus.redis.datasource.stream.XAddArgs;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Startup
@ApplicationScoped
public class LogConsumer {
    private static final Logger LOGGER = Logger.getLogger(LogConsumer.class);
    private final RedisDataSource redisDS;
    private final ExecutorService executorService;

    public LogConsumer(RedisDataSource redisDS) {
        this.redisDS = redisDS;
        this.executorService = Executors.newSingleThreadExecutor();
    }

    void onStart(@Observes StartupEvent ev) {
        LOGGER.info("The application is starting...");
        this.executorService.submit(new RedisWorker(this.redisDS));
    }

    void onStop(@Observes ShutdownEvent ev) throws InterruptedException {
        LOGGER.info("The application is stopping...");
        this.executorService.shutdownNow();
        this.executorService.awaitTermination(5, TimeUnit.SECONDS);
    }

    public void send(Map<String, String> words) {
        this.redisDS.stream(String.class).xadd("mystream", new XAddArgs(), words);
    }

    private static class RedisWorker implements Runnable {

        private String lastReadId = ">";
        private final RedisDataSource redisDS;
        public final StreamCommands<String, String, String> streamCommands;

        public RedisWorker(RedisDataSource redisDS) {
            this.redisDS = redisDS;
            this.streamCommands = this.redisDS.stream(String.class);
        }

        @Override
        public void run() {

            while (true) {
                LOGGER.info("read redis stream...");
                List<StreamMessage<String, String, String>> messages =
                        streamCommands.xreadgroup("app_1", "consumer_1", "mystream",
                                ">");
                if (!messages.isEmpty()) {
                    LOGGER.info("size: " + messages.size());
                    for (StreamMessage<String, String, String> message : messages) {
                        LOGGER.info("id: " + message.id() + " key: " + message.key() + System.lineSeparator()
                                + "payload: " + new ArrayList<>(message.payload().values()));
                        // Confirm that the message has been processed using XACK
                        this.streamCommands.xack("mystream", "app_1", message.id());
                    }
                } else {
                    long mystream = streamCommands.xlen("mystream");
                    LOGGER.info("Stream size: " + mystream);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }
}
