package htsjdk.samtools.util.async;

import java.util.concurrent.*;
import java.util.function.Consumer;

public class AsyncConsumer<T> {

    private final BlockingQueue<Wrapper<T>> blockingQueue;
    private final ExecutorService executorService;
    private final CountDownLatch downLatch;

    private static final int DEFAULT_THREAD_AMOUNT = 1;
    private static final int DEFAULT_QUEUE_SIZE = 1;

    private AsyncConsumer(Consumer<T> consumer, int queueSize, int nThreads) {
        this.blockingQueue = new ArrayBlockingQueue<>(queueSize);
        this.downLatch = new CountDownLatch(nThreads);
        this.executorService = createThreads(nThreads, consumer);
    }

    static public <T> AsyncConsumer<T> getInstance(Consumer<T> consumer, int queueSize, int nThreads) {
        return new AsyncConsumer<>(consumer, queueSize, nThreads);
    }

    static public <T> AsyncConsumer<T> getInstance(Consumer<T> consumer, int queueSize) {
        return new AsyncConsumer<>(consumer, queueSize, DEFAULT_THREAD_AMOUNT);
    }

    static public <T> AsyncConsumer<T> getInstance(Consumer<T> consumer) {
        return new AsyncConsumer<>(consumer, DEFAULT_QUEUE_SIZE, DEFAULT_THREAD_AMOUNT);
    }

    static public <T> AsyncConsumer<T> getInstance(int nThreads, Consumer<T> consumer) {
        return new AsyncConsumer<>(consumer, nThreads, nThreads);
    }

    public void finish() throws InterruptedException {
        long threads = downLatch.getCount();
        for(int i = 0; i < threads; i++) {
            //sending sign of end processing
            blockingQueue.put(Wrapper.getStopInstance());
        }

        downLatch.await();
    }

    public void put(T record) throws InterruptedException {
        if (record == null) {
            throw new NullPointerException("Value must be not null");
        }
        blockingQueue.put(Wrapper.getInstance(record));
    }

    private ExecutorService createThreads(int nThreads, Consumer<T> consumer) {
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        for(int i = 0; i < nThreads; i ++) {
            executor.execute(new ConsumerTask<>(blockingQueue, consumer, downLatch));
        }
        executor.shutdown();
        return executor;
    }

    private static class ConsumerTask<T> implements Runnable {

        private final BlockingQueue<Wrapper<T>> queue;
        private final Consumer<T> consumer;
        private final CountDownLatch latch;

        private ConsumerTask(BlockingQueue<Wrapper<T>> queue, Consumer<T> consumer,
                             CountDownLatch latch) {
            this.queue = queue;
            this.consumer = consumer;
            this.latch = latch;
        }

        @Override
        public void run() {
            boolean isNotInterrupted = true;
            while (isNotInterrupted) {
                try {
                    T elem =  queue.take().getRecord();
                    if (elem == null) {
                        break;
                    }
                    consumer.accept(elem);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    isNotInterrupted = false;
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
            latch.countDown();
        }
    }

    private static class Wrapper<T> {

        private final T record;

        private Wrapper(T record) {
            this.record = record;
        }

        public T getRecord() {
            return record;
        }

        public static <T> Wrapper<T> getInstance(T record) {
            return new Wrapper<>(record);
        }

        public static <T> Wrapper<T> getStopInstance() {
            return new Wrapper<>(null);
        }
    }
}
