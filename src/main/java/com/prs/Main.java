package com.prs;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        Queue queue = new Queue();
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        Topic t1 = queue.createTopic("t1", executorService);
        Topic t2 = queue.createTopic("t2", executorService);

        TopicSubscriber subscriber1 = new TopicSubscriber("subscriber1");
        TopicSubscriber subscriber2 = new TopicSubscriber("subscriber2");
        TopicSubscriber subscriber3 = new TopicSubscriber("subscriber3");

        queue.subscribeTopic(subscriber1, t1);
        queue.subscribeTopic(subscriber2, t1);

        queue.subscribeTopic(subscriber3, t2);

        Thread.sleep(1000);

        queue.publishMessage(t1, "m1");
        queue.publishMessage(t1, "m2");
//        queue.publishMessage(t1, "m4");
//        queue.publishMessage(t1, "m5");

        queue.publishMessage(t2, "m3");

        queue.reset(subscriber1, t1, 0);



        // proper way to shutdown threadpool OR in jdk19 executorService implements AutoCloseable, meaning it shuts down when exiting a try-with-resources block:
        executorService.shutdown();// Disable new tasks from being submitted

        try {
            if (!executorService.awaitTermination(4000, TimeUnit.MILLISECONDS)) {// Wait a while for existing tasks to terminate
                executorService.shutdownNow();// Cancel currently executing tasks

                if (!executorService.awaitTermination(4000, TimeUnit.MILLISECONDS)) { // Wait a while for tasks to respond to being cancelled
                    System.err.println("Pool did not terminate");
                }
            }
        }
        catch (InterruptedException ex) {
            executorService.shutdownNow();// (Re-)Cancel if current thread also interrupted
            Thread.currentThread().interrupt();// Preserve interrupt status
        }

    }
}