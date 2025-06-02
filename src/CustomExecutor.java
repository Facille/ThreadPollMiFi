import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public interface CustomExecutor extends java.util.concurrent.Executor {
    void execute(Runnable command);
    <T> Future<T> submit(Callable<T> callable);
    void shutdown();
    void shutdownNow();
}