import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        CustomThreadPool.CustomThreadFactory factory =
                new CustomThreadPool.CustomThreadFactory("CustomPool");

        CustomThreadPool pool = new CustomThreadPool(
                2,  // corePoolSize
                4,  // maxPoolSize
                5,  // keepAliveTime
                TimeUnit.SECONDS,
                5,  // queueSize
                1,  // minSpareThreads
                factory
        );

        // Отправка задач в пул
        for (int i = 0; i < 20; i++) {
            final int taskId = i;
            try {
                pool.execute(() -> {
                    System.out.printf("Task %d started%n", taskId);
                    try {
                        Thread.sleep(1000 + (int)(Math.random() * 2000));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    System.out.printf("Task %d completed%n", taskId);
                });
            } catch (Exception e) {
                System.err.println("Error submitting task: " + e.getMessage());
            }
            Thread.sleep(200);
        }

        // Демонстрация перегрузки
        for (int i = 20; i < 30; i++) {
            final int taskId = i;
            pool.execute(() -> {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // Завершение работы пула
        Thread.sleep(5000);
        pool.shutdown();

        // Ожидание завершения всех задач
        Thread.sleep(7000);
        System.out.println("All tasks processed");
    }
}