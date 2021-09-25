package ru.vichukano.ignite.server;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;

import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.EventType;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import java.util.concurrent.TimeUnit;

public class IgniteServer {
    private static final String CACHE_NAME = "ignite listener";

    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("src/main/resources/ignite.xml")) {
            System.out.println("-----------IGNITE SERVER STARTED---------------");
            CacheConfiguration<Integer, String> cc = new CacheConfiguration<>();
            cc.setName(CACHE_NAME).setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, 2)));
            cc.setEagerTtl(true);
            cc.setStatisticsEnabled(true);
            ignite.createCache(cc);
            System.out.println("-----------IGNITE CACHE CREATED---------------");
            ClientConfiguration cfg = new ClientConfiguration().setAddresses("127.0.0.1:10800");
            try (IgniteClient client = Ignition.startClient(cfg)) {
                System.out.println("-----------IGNITE THIN CLIENT STARTED---------------");
                ClientCache<Integer, String> cache = client.getOrCreateCache(CACHE_NAME);
                ContinuousQuery<Integer, String> query = new ContinuousQuery<>();
                query.setIncludeExpired(true);
                query.setLocalListener(events -> events.forEach(e -> System.out.println(
                        "EVENT: "
                                + e.getEventType()
                                + " "
                                + e.getKey()
                                + " "
                                + e.getValue())));
                //Filter fo expired entity
                query.setRemoteFilterFactory((Factory<CacheEntryEventFilter<Integer, String>>) () -> e -> true);
                cache.query(query);
                System.out.println("-----------INSERT---------------");
                cache.put(1, "one");
                cache.put(2, "two");
                cache.put(3, "three");
                System.out.println("-----------CACHE POPULATED---------------");
            }
            Thread.sleep(20000);
        }
    }
}
