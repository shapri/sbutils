package deadlines;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class Poller implements Callable<Integer> {

    private final long timeout;
    private final CountDownLatch countDownLatch;
    private final DeadlineEngine engine;
    private final int maxPoll;
    private final Consumer<Long> consumer;
    private int pollSize = 0;

    Poller(DeadlineEngine engine, CountDownLatch countDownLatch, long timeout, int maxPoll, Consumer<Long> consumer) {
        this.engine = engine;
        this.countDownLatch = countDownLatch;
        this.timeout = timeout;
        this.maxPoll = maxPoll;
        this.consumer = consumer;
    }

    @Override
    public Integer call(){

        while(true){

            pollSize += engine.poll(System.currentTimeMillis(), consumer, maxPoll);

            if( System.currentTimeMillis() > timeout ) {
                countDownLatch.countDown();
                break;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return pollSize;
    }
}
