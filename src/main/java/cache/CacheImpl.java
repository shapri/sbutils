package cache;

import java.util.concurrent.*;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheImpl<K, V> implements Cache<K, V> {

    Logger LOG = LoggerFactory.getLogger(CacheImpl.class);

    private final Function<K, V> calculator;

    private final ConcurrentMap<K, Future<V>> cache = new ConcurrentHashMap<>();

    public CacheImpl(Function<K, V> calculator) {
        this.calculator = calculator;
    }

    @Override
    public V get(K key){

        Future<V> future = getOrPutFuture(key);
        try {
            return future.get();
        } catch (InterruptedException e) {
            LOG.debug("InterruptedException when calculator called", e);
            cache.remove(key, future);
            LOG.debug("resetting cache {}", key);
            throw new RuntimeException(e.getCause()); // wrap inside runtime exception to avoid modifying interface
        } catch (ExecutionException e) {
            LOG.debug("ExecutionException when calculator called", e);
            //cache.remove(key, future); // not allowing a retry - could make this a configurable property
            throw new RuntimeException(e.getCause()); // wrap inside runtime exception to avoid modifying interface
        }
    }

    private Future<V> getOrPutFuture(K key) {

        Future<V> future = cache.get(key);

        if(future == null) {
            FutureTask<V> task = new FutureTask(() -> calculator.apply(key));
            future = cache.putIfAbsent(key, task);

            if(future == null) {
                future = cache.put(key, task);
                LOG.debug("future task cached for {}", key);
                task.run();
                LOG.debug("future task run for {}", key);
            }
        }

        return future;
    }
}