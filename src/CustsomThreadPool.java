import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class CustomThreadPool implements CustomExecutor {

    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int queueSize;
    private final int minSpareThreads;


    private final AtomicInteger currentPoolSize = new AtomicInteger(0);
    private final AtomicInteger idleCount = new AtomicInteger(0);
    private volatile boolean isShutdown = false;
    private volatile boolean isTerminating = false;

    private final List<BlockingQueue<Runnable>> workQueues = new CopyOnWriteArrayList<>();
    private final List<Worker> workers = new CopyOnWriteArrayList<>();
    private final CustomThreadFactory threadFactory;
    private final RejectedTaskHandler rejectedHandler;
    private final AtomicInteger nextQueueIndex = new AtomicInteger(0);

    private final AtomicLong completedTaskCount = new AtomicLong(0);
    private final AtomicLong rejectedTaskCount = new AtomicLong(0);

    public CustomThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime,
                            TimeUnit timeUnit, int queueSize, int minSpareThreads,
                            CustomThreadFactory threadFactory) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;
        this.threadFactory = threadFactory;
        this.rejectedHandler = new RejectedTaskHandler();

        for (int i = 0; i < corePoolSize; i++) {
            createNewWorker();
        }
    }

    @Override
    public void execute(Runnable command) {
        if (isShutdown) {
            rejectedHandler.reject(command, "Pool is shutting down");
            return;
        }

        ensureMinSpareThreads();

        boolean taskAdded = false;
        final int startIdx = nextQueueIndex.getAndUpdate(i -> (i + 1) % workQueues.size());
        int idx = startIdx;

        do {
            BlockingQueue<Runnable> queue = workQueues.get(idx);
            if (queue.offer(command)) {
                logTaskAccepted(command, idx);
                taskAdded = true;
                break;
            }
            idx = (idx + 1) % workQueues.size();
        } while (idx != startIdx);

        if (!taskAdded && currentPoolSize.get() < maxPoolSize) {
            createNewWorker();
            BlockingQueue<Runnable> newQueue = workQueues.get(workQueues.size() - 1);
            if (newQueue.offer(command)) {
                logTaskAccepted(command, workQueues.size() - 1);
                taskAdded = true;
            }
        }

        if (!taskAdded) {
            rejectedHandler.handle(command);
        }
    }

    @Override
    public <T> java.util.concurrent.Future<T> submit(java.util.concurrent.Callable<T> callable) {
        java.util.concurrent.FutureTask<T> future = new java.util.concurrent.FutureTask<>(callable);
        execute(future);
        return future;
    }

    @Override
    public void shutdown() {
        isShutdown = true;
        workers.forEach(Worker::softStop);
    }

    @Override
    public void shutdownNow() {
        isTerminating = true;
        workers.forEach(Worker::forceStop);
        workQueues.forEach(BlockingQueue::clear);
    }

    private void createNewWorker() {
        if (currentPoolSize.get() >= maxPoolSize) return;

        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(queueSize);
        workQueues.add(queue);

        Worker worker = new Worker(queue);
        workers.add(worker);

        Thread thread = threadFactory.newThread(worker);
        thread.start();

        currentPoolSize.incrementAndGet();
        idleCount.incrementAndGet();
    }

    private void ensureMinSpareThreads() {
        while (idleCount.get() < minSpareThreads &&
                currentPoolSize.get() < maxPoolSize &&
                !isShutdown) {
            createNewWorker();
        }
    }

    private void logTaskAccepted(Runnable task, int queueId) {
        System.out.printf("[Pool] Task %s accepted into queue #%d%n",
                task.toString(), queueId);
    }

    public void logWorkerIdleTimeout(Thread thread) {
        System.out.printf("[Worker] %s idle timeout, stopping.%n", thread.getName());
    }

    public void logTaskRejected(Runnable task) {
        System.out.printf("[Rejected] Task %s was rejected due to overload!%n", task.toString());
    }

    public void logTaskExecuted(Thread thread, Runnable task) {
        System.out.printf("[Worker] %s executes %s%n", thread.getName(), task.toString());
    }

    public void logWorkerTerminated(Thread thread) {
        System.out.printf("[Worker] %s terminated.%n", thread.getName());
    }

    private class Worker implements Runnable {
        private final BlockingQueue<Runnable> taskQueue;
        private volatile boolean running = true;
        private volatile boolean processing = false;

        Worker(BlockingQueue<Runnable> taskQueue) {
            this.taskQueue = taskQueue;
        }

        @Override
        public void run() {
            Thread currentThread = Thread.currentThread();
            try {
                while (running && !currentThread.isInterrupted()) {
                    try {
                        Runnable task = taskQueue.poll(keepAliveTime, timeUnit);
                        if (task != null) {
                            idleCount.decrementAndGet();
                            processing = true;
                            logTaskExecuted(currentThread, task);
                            task.run();
                            completedTaskCount.incrementAndGet();
                        } else if (shouldTerminate()) {
                            break;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        if (!processing) {
                            idleCount.incrementAndGet();
                        }
                        processing = false;
                    }
                }
            } finally {
                cleanupWorker(currentThread);
            }
        }

        private boolean shouldTerminate() {
            return currentPoolSize.get() > corePoolSize &&
                    idleCount.get() > minSpareThreads;
        }

        private void cleanupWorker(Thread thread) {
            workers.remove(this);
            workQueues.remove(taskQueue);
            currentPoolSize.decrementAndGet();
            if (processing) {
                idleCount.decrementAndGet();
            }
            logWorkerTerminated(thread);
        }

        void softStop() {
            running = false;
        }

        void forceStop() {
            running = false;
            Thread.currentThread().interrupt();
        }
    }

    private class RejectedTaskHandler {
        public void handle(Runnable task) {
            if (!isShutdown && !isTerminating) {
                reject(task, "System overload");
            }
        }

        private void reject(Runnable task, String reason) {
            rejectedTaskCount.incrementAndGet();
            logTaskRejected(task);
            System.err.printf("Rejection reason: %s%n", reason);
        }
    }

    public static class CustomThreadFactory implements java.util.concurrent.ThreadFactory {
        private final String poolName;
        private final AtomicInteger threadCount = new AtomicInteger(1);

        public CustomThreadFactory(String poolName) {
            this.poolName = poolName;
        }

        @Override
        public Thread newThread(Runnable r) {
            String threadName = poolName + "-worker-" + threadCount.getAndIncrement();
            System.out.printf("[ThreadFactory] Creating new thread: %s%n", threadName);
            return new Thread(r, threadName);
        }
    }
}