package cache;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.lang.reflect.Field;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.reset;
import static org.powermock.api.support.membermodification.MemberMatcher.method;

@RunWith(PowerMockRunner.class)
@PrepareForTest(CacheImpl.class)
public class CacheImplTest {

    @Test
    public void calculatorShouldOnlyBeCalledOnceWithTwoThreads() throws InterruptedException {

        AtomicInteger calculatorEvents = new AtomicInteger();
        Function<String, Integer> calculator = (k) -> {

            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            calculatorEvents.addAndGet(1);
            return k.length();
        };

        Cache<String, Integer> cache = new CacheImpl<String, Integer>(calculator);
        CountDownLatch countdownLatch = new CountDownLatch(2);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(() -> {cache.get("Hello"); countdownLatch.countDown();});
        executor.submit(() -> {cache.get("Hello"); countdownLatch.countDown();});

        countdownLatch.await();
        assertThat(calculatorEvents.get()).isEqualTo(1);
        assertThat(cache.get("Hello")).isEqualTo(5);
    }

    @Test
    public void nullValueCheck() throws InterruptedException {

        AtomicInteger calculatorEvents = new AtomicInteger();
        Function<String, Integer> calculator = (k) -> {

            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            calculatorEvents.addAndGet(1);
            return null;
        };

        Cache<String, Integer> cache = new CacheImpl<String, Integer>(calculator);
        CountDownLatch countdownLatch = new CountDownLatch(2);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(() -> {cache.get("Hello"); countdownLatch.countDown();});
        executor.submit(() -> {cache.get("Hello"); countdownLatch.countDown();});

        countdownLatch.await();
        assertThat(calculatorEvents.get()).isEqualTo(1);
        assertThat(cache.get("Hello")).isNull();
    }

    @Test()
    public void calculatorInterruptedShouldNotCacheAllowingRetry() throws Exception {

        AtomicInteger calculatorEvents = new AtomicInteger();
        CountDownLatch countdownLatch = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(1);

        Function<String, Integer> calculator = (k) -> {

            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            calculatorEvents.addAndGet(1);
            countdownLatch.countDown();
            return Integer.valueOf(99);
        };

        Cache<String, Integer> cache = new CacheImpl<String, Integer>(calculator);
        Cache<String, Integer> cacheSpy = PowerMockito.spy(cache);

        Future<Integer> mockFuture = (Future<Integer>)PowerMockito.mock(Future.class);
        PowerMockito.when(mockFuture.get()).thenThrow(new InterruptedException());

        PowerMockito.doReturn(mockFuture).when(cacheSpy,
                method(CacheImpl.class, "getOrPutFuture", String.class))
                .withArguments(anyString());

        try {
            cacheSpy.get("Hello");
        } catch(Throwable e ){
            assertThat(calculatorEvents.get()).isEqualTo(0);
        }

        // now reset mocks and try again
        reset(mockFuture);
        reset(cacheSpy);
        cacheSpy.get("Hello");
        assertThat(calculatorEvents.get()).isEqualTo(1);
    }

    @Test()
    public void calculatorExecutionExceptionShouldNotCacheAllowingRetry() throws Exception {

        AtomicInteger calculatorEvents = new AtomicInteger();
        CountDownLatch countdownLatch = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(1);

        Function<String, Integer> calculator = (k) -> {

            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            calculatorEvents.addAndGet(1);
            throw new RuntimeException("ExecutionException");
        };

        Cache<String, Integer> cache = new CacheImpl<String, Integer>(calculator);

        try {
            cache.get("Hello");
        } catch(Throwable e ){
            assertThat(calculatorEvents.get()).isEqualTo(1);
        }

        try {
            cache.get("Hello");
        } catch(Throwable e ){
            assertThat(calculatorEvents.get()).isEqualTo(1); // should still be one
        }
    }


    @Test
    public void calculatorShouldOnlyBeCalledOnceWithTwoThreadsStressTest() throws InterruptedException {

        AtomicInteger calculatorEvents = new AtomicInteger();
        Function<String, Integer> calculator = (k) -> {

            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            calculatorEvents.addAndGet(1);
            return k.length();
        };

        int iterations = 100_000;
        Cache<String, Integer> cache = new CacheImpl<>(calculator);
        CountDownLatch countdownLatch = new CountDownLatch(2);

        IntStream.iterate(0, i -> i<iterations, i -> i + 1)
                .parallel()
                .forEach( e -> {cache.get("TEST");countdownLatch.countDown();});

        countdownLatch.await();
        assertThat(calculatorEvents.get()).isEqualTo(1);
    }
}