package com.prs;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

public class Queue {
    private final ConcurrentHashMap<String, Topic> topicMap;

    public Queue() {
        this.topicMap = new ConcurrentHashMap<>();
    }

    public Topic createTopic(String topicName, final Executor threadpool) {
        Topic topic = new Topic(UUID.randomUUID().toString(), topicName, threadpool);
        this.topicMap.put(topic.getTopicId(), topic);
        System.out.println("Created topic : "+topicName);
        return topic;
    }

    public void publishMessage(Topic topic, String message) {
//        topic.publishMessage(message);
        new Thread(()->topic.publishMessage(message)).start();
        System.out.println("Message "+message+ " published to topic "+topic.getTopicName());
    }

    public void subscribeTopic( TopicSubscriber subscriber, Topic topic) {
        this.topicMap.get(topic.getTopicId()).subscribe(subscriber);
        System.out.println("Susbcriber "+subscriber.getSubscriberId()+" subscribed to topic "+ topic.getTopicName());
    }

    public void reset(TopicSubscriber subscriber, Topic topic, int offset) {
        for (int i = 0; i < topic.getSubscribers().size(); i++) {
            if (subscriber.equals(topic.getSubscribers().get(i))) {
                subscriber.getOffset().set(offset);
                System.out.println(subscriber.getSubscriberId()+" offset for topic "+topic.getTopicName()+" resetted to "+offset);
                topic.fanOut(subscriber);
                break;
            }
        }
//        synchronized (subscriber) {
//            subscriber.getOffset().set(offset);
//            subscriber.notifyAll();
//        }
    }
}
