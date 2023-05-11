package org.example.consumer;

import io.quarkus.redis.datasource.stream.StreamCommands;
import org.jboss.logging.Logger;

public class RedisCreateConsumerGroup implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(RedisCreateConsumerGroup.class);
    private final String stream;
    private final String consumerGroup;
    private final StreamCommands<String, String, String> streamCommands;

    public RedisCreateConsumerGroup(String stream, String consumerGroup, StreamCommands<String, String, String> streamCommands) {
        this.stream = stream;
        this.consumerGroup = consumerGroup;
        this.streamCommands = streamCommands;
    }

    @Override
    public void run() {
        try {
            streamCommands.xgroupCreate(stream, consumerGroup, "0-0");
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }
}
