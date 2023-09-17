package com.prs;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

@Getter
public class Topic {
    private final String topicId;
    private final String topicName;

    private final List<String> messages;

    private final List<TopicSubscriber> subscribers;

    private final Map<String, FanoutTask> subscriberWorker;

    private final Executor threadpool;

    public Topic(String topicId, String topicName, Executor threadpool) {
        this.topicId = topicId;
        this.topicName = topicName;
        this.messages = Collections.synchronizedList(new ArrayList<>());
        this.subscribers = Collections.synchronizedList(new ArrayList<>());
        this.threadpool = threadpool;
        this.subscriberWorker = new ConcurrentHashMap<>();
    }


    public void publishMessage(String message) {
//        synchronized (this) {
            this.messages.add(message);
            for (TopicSubscriber subscriber : subscribers) {
                fanOut(subscriber);
            }
//        }
    }

    public void fanOut(TopicSubscriber subscriber) {
        FanoutTask task = this.subscriberWorker.get(subscriber.getSubscriberId());
        if (task == null) {
            throw new IllegalStateException("No Fanout Task available");
        }
        synchronized (subscriber) {
            subscriber.notify();
        }
    }

    public void subscribe(TopicSubscriber subscriber) {
//        synchronized (this) {
        this.subscribers.add(subscriber);
        FanoutTask task = new FanoutTask(subscriber, this);
        this.subscriberWorker.putIfAbsent(subscriber.getSubscriberId(), task);
        this.threadpool.execute(task);
//        }
    }
}
