package com.prs;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.atomic.AtomicInteger;

@Getter
@Setter
public class TopicSubscriber {
    private final String subscriberId;
    private AtomicInteger offset;

    public TopicSubscriber(String subscriberId) {
        this.subscriberId = subscriberId;
        this.offset = new AtomicInteger(0);
    }

    public void consumeMessage(String message) throws InterruptedException {
        Thread.sleep(1000);
        System.out.println(message+" : consumed by : "+subscriberId);
    }
}
