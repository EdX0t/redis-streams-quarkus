package org.example.consumer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.stream.StreamCommands;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Startup
@ApplicationScoped
public class LogConsumer {
    private static final Logger LOGGER = Logger.getLogger(LogConsumer.class);
    public static final String CONSUMER_GROUP = "app_1";
    public static final String CONSUMER = "consumer_1";
    public static final String STREAM = "mystream";
    private final StreamCommands<String, String, String> streamCommands;
    private final ExecutorService executorService;
    private AtomicBoolean running;

    @Inject
    ElasticsearchClient client;

    public LogConsumer(RedisDataSource redisDS) {
        this.streamCommands = redisDS.stream(String.class);
        this.executorService = Executors.newSingleThreadExecutor();
    }

    void onStart(@Observes StartupEvent ev) throws InterruptedException {
        LOGGER.info("The application is starting...");
        this.executorService.submit(new RedisCreateConsumerGroup(STREAM, CONSUMER_GROUP, streamCommands));
        this.running = new AtomicBoolean(true);
        this.executorService.submit(new RedisWorker(STREAM, CONSUMER_GROUP, CONSUMER, streamCommands, client, running));
    }

    void onStop(@Observes ShutdownEvent ev) throws InterruptedException {
        LOGGER.info("The application is stopping...");
        this.running.getAndSet(false);
        this.executorService.shutdown();
        this.executorService.awaitTermination(5, TimeUnit.SECONDS);
    }

}
