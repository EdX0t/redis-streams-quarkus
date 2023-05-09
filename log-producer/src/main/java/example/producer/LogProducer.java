package example.producer;

import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.stream.XAddArgs;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.Map;

@ApplicationScoped
public class LogProducer {
    private static final Logger LOGGER = Logger.getLogger(LogProducer.class);
    private final RedisDataSource redisDS;

    public LogProducer(RedisDataSource redisDS) {
        this.redisDS = redisDS;
    }

    public void send(Map<String,String> words) {
        this.redisDS.stream(String.class).xadd("mystream", new XAddArgs(), words);
    }
}
