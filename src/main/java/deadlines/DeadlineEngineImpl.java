package deadlines;

import cache.CacheImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class DeadlineEngineImpl implements DeadlineEngine {

    Logger LOG = LoggerFactory.getLogger(DeadlineEngineImpl.class);

    private final ConcurrentNavigableMap<Long, Set<Long>> deadlines = new ConcurrentSkipListMap<>();

    private final AtomicLong requestIdCounter = new AtomicLong();

    @Override
    public long schedule(long deadlineMs) {

        if(deadlineMs < 0 )
            throw new IllegalArgumentException("deadlineMs must be greater than 0");

        long requestId = requestIdCounter.addAndGet(1);
        deadlines.computeIfAbsent(deadlineMs, (k) -> new ConcurrentHashMap<Long,Boolean>().newKeySet() );
        deadlines.get(deadlineMs).add(requestId);

        LOG.debug("scheduled: {} ms", deadlineMs);
        return requestId;
    }

    @Override
    public boolean cancel(long requestId) {

        for( long deadlineMs : deadlines.keySet() ){
            Set<Long> requestIds = deadlines.get(deadlineMs);
            if( requestIds.remove(requestId) ) {
                deadlines.computeIfPresent(deadlineMs, (k, v) -> v.size() == 0 ? null : v);
                LOG.debug("cancelled requestId: {}", requestId);
                return true;
            }
        }
        return false;
    }

    @Override
    public int poll(long nowMs, Consumer<Long> handler, int maxPoll) {

        Objects.requireNonNull(handler);

        List<Long> expired = new ArrayList<>();

        for( long deadlineMs : deadlines.keySet() ) { /* keyset should be in ascending order */

            if(deadlineMs > nowMs || expired.size() >= maxPoll)
                break;

            Set<Long> requestIds = deadlines.get(deadlineMs);

            if(requestIds.size() == 0) // remove map entry
                deadlines.computeIfPresent(deadlineMs, (k, v) -> v.size() == 0 ? null : v);

            for(long requestId : requestIds) {

                if(expired.size() >= maxPoll ) // limit to maxPoll
                    break;

                if( requestIds.remove(requestId) )
                    expired.add(requestId);
            }
        }

        expired.parallelStream().mapToLong(Long::longValue).forEach(e -> handler.accept(e));

        LOG.debug("poll expired: {}", expired.size());

        return expired.size();
    }

    @Override
    public int size() {

        return deadlines.values()
                .stream()
                .map(e -> e.size())
                .mapToInt(Integer::intValue)
                .sum();
    }
}
