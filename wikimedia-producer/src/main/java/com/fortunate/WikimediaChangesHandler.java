package com.fortunate;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

@AllArgsConstructor
public class WikimediaChangesHandler implements EventHandler {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    private static final Logger LOG = LoggerFactory.getLogger(WikimediaChangesHandler.class);


    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {

    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        LOG.info(String.format("#### -> Event data -> %s", messageEvent.getData()));
        this.kafkaTemplate.send(topic, messageEvent.getData());
    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {

    }
}
