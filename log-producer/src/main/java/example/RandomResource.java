package example;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import example.producer.LogProducer;

import java.util.*;

@Path("/test")
public class RandomResource {

    public static final List<String> WORDS = Arrays.asList(
            "confess",
            "care",
            "pocket",
            "nod",
            "disarm",
            "blue",
            "line",
            "interrupt",
            "familiar",
            "sofa",
            "error",
            "worm");

    @Inject
    LogProducer logProducer;

    @GET
    public List<String> keys() {
        Map<String,String> words = new HashMap<>();
        for(int i = 1; i < 11; i++) {
            Random r = new Random();
            words.put(String.valueOf(i), WORDS.get(r.nextInt(WORDS.size())));
        }
        logProducer.send(words);
        return new ArrayList<>(words.values());
    }

}
