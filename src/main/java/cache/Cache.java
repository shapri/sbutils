package cache;

import java.util.concurrent.ExecutionException;

public interface Cache<K, V> {
    V get(K key);
}
