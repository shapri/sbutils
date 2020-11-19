package deadlines;

import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class DeadlineEngineImplTest {

    @Test
    public void shouldHaveThreeDeadlinesAllAtTheSameTime() throws InterruptedException {

        AtomicInteger handlerEvents = new AtomicInteger();
        Consumer<Long> handler = e -> handlerEvents.addAndGet(1);
        DeadlineEngine engine = new DeadlineEngineImpl();
        long t0 = System.currentTimeMillis();
        int numberOfThreads = 4;
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        CountDownLatch countDownLatch = new CountDownLatch(numberOfThreads);

        for( int i=0; i<numberOfThreads; i++)
            executor.submit( new Poller(engine, countDownLatch, t0 + 5_000, 4, handler) );

        long id1 = engine.schedule(t0 + 15_000);
        long id2 = engine.schedule(t0 + 15_000);
        long id3 = engine.schedule(t0 + 15_000);

        countDownLatch.await();

        assertThat(engine.size()).isEqualTo(3);
        assertThat(handlerEvents.get()).isEqualTo(0);
        assertThat(engine.cancel(id1)).isTrue();
        assertThat(engine.cancel(id2)).isTrue();
        assertThat(engine.cancel(id3)).isTrue();
    }

    @Test
    public void shouldHaveThreeDeadlinesTwoAtTheSameTime() throws InterruptedException {

        AtomicInteger handlerEvents = new AtomicInteger();
        Consumer<Long> handler = e -> handlerEvents.addAndGet(1);
        DeadlineEngine engine = new DeadlineEngineImpl();
        long t0 = System.currentTimeMillis();
        int numberOfThreads = 4;
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        CountDownLatch countDownLatch = new CountDownLatch(numberOfThreads);

        long id1 = engine.schedule(t0 + 15_000);
        long id2 = engine.schedule(t0 + 15_000);
        long id3 = engine.schedule(t0 + 20_000);

        for( int i=0; i<numberOfThreads; i++)
            executor.submit( new Poller(engine, countDownLatch, t0 + 5_000, 4, handler) );

        countDownLatch.await();
        assertThat(engine.size()).isEqualTo(3);
        assertThat(handlerEvents.get()).isEqualTo(0);
        assertThat(engine.cancel(id1)).isTrue();
        assertThat(engine.cancel(id2)).isTrue();
        assertThat(engine.cancel(id3)).isTrue();
    }

    @Test
    public void shouldHaveOneDeadlineLeftAfterTwoExpire() throws InterruptedException {

        AtomicInteger handlerEvents = new AtomicInteger();
        Consumer<Long> handler = e -> handlerEvents.addAndGet(1);
        DeadlineEngine engine = new DeadlineEngineImpl();
        long t0 = System.currentTimeMillis();
        int numberOfThreads = 4;
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        CountDownLatch countDownLatch = new CountDownLatch(numberOfThreads);

        long id1 = engine.schedule(t0 - 3_000);
        long id2 = engine.schedule(t0 - 3_000);
        long id3 = engine.schedule(t0 + 3_000);

        for( int i=0; i<numberOfThreads; i++)
            executor.submit( new Poller(engine, countDownLatch, t0, 4, handler) );

        countDownLatch.await();
        assertThat(engine.size()).isEqualTo(1);
        assertThat(engine.cancel(id1)).isFalse();
        assertThat(engine.cancel(id2)).isFalse();
        assertThat(engine.cancel(id3)).isTrue();
        assertThat(handlerEvents.get()).isEqualTo(2);
    }

    @Test
    public void shouldHaveTwoDeadlinesLeftAfterPollingForTwoUsingOneThread() throws InterruptedException {

        AtomicInteger handlerEvents = new AtomicInteger();
        Consumer<Long> handler = e -> handlerEvents.addAndGet(1);
        DeadlineEngine engine = new DeadlineEngineImpl();
        long t0 = System.currentTimeMillis();

        long id1 = engine.schedule(t0 - 3_000);
        long id2 = engine.schedule(t0 - 3_000);
        long id3 = engine.schedule(t0 - 3_000);
        long id4 = engine.schedule(t0 - 3_000);

        engine.poll(System.currentTimeMillis(), handler, 2);

        assertThat(engine.size()).isEqualTo(2);
        assertThat(engine.cancel(id1)).isFalse();
        assertThat(engine.cancel(id2)).isFalse();
        assertThat(engine.cancel(id3)).isTrue();
        assertThat( engine.cancel(id4)).isTrue();
        assertThat(handlerEvents.get()).isEqualTo(2);
    }

    @Test
    public void shouldHaveNoDeadlinesLeftAfterPollingForFourUsingOneThread() throws InterruptedException {

        AtomicInteger handlerEvents = new AtomicInteger();
        Consumer<Long> handler = e -> handlerEvents.addAndGet(1);
        DeadlineEngine engine = new DeadlineEngineImpl();
        long t0 = System.currentTimeMillis();

        long id1 = engine.schedule(t0 - 3_000);
        long id2 = engine.schedule(t0 - 3_000);
        long id3 = engine.schedule(t0 - 3_000);
        long id4 = engine.schedule(t0 - 3_000);

        engine.poll(System.currentTimeMillis(), handler, 4);

        assertThat(engine.size()).isEqualTo(0);
        assertThat(handlerEvents.get()).isEqualTo(4);
    }

    @Test
    public void validCancel() throws InterruptedException {

        AtomicInteger handlerEvents = new AtomicInteger();
        DeadlineEngine engine = new DeadlineEngineImpl();
        long t0 = System.currentTimeMillis();
        int numberOfThreads = 4;
        long id1 = engine.schedule(t0 + 15_000);
        assertThat(engine.cancel(id1)).isTrue();
    }

    @Test
    public void invalidCancel() throws InterruptedException {

        AtomicInteger handlerEvents = new AtomicInteger();
        DeadlineEngine engine = new DeadlineEngineImpl();
        long t0 = System.currentTimeMillis();
        long id1 = engine.schedule(t0 + 15_000);
        assertThat(engine.cancel(-1)).isFalse();
    }
}