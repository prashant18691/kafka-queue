package com.prs;

public class FanoutTask implements Runnable {
    private final TopicSubscriber subscriber;

    private final Topic topic;

    public FanoutTask(TopicSubscriber subscriber, Topic topic) {
        this.subscriber = subscriber;
        this.topic = topic;
    }

    @Override
    public void run() {
        synchronized (subscriber) {
            while (true) {
                int currOffset = subscriber.getOffset().get();

                try {
                    while (this.topic.getMessages().size() <= currOffset) {
                        subscriber.wait();
                    }
                    subscriber.consumeMessage(this.topic.getMessages().get(currOffset));
                    subscriber.getOffset().compareAndSet(currOffset, currOffset + 1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }
}
