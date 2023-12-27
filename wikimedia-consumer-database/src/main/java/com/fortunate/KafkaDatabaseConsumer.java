package com.fortunate;

import com.fortunate.entity.WikimediaData;
import com.fortunate.repository.WikimediaDataRepository;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class KafkaDatabaseConsumer {
    private final WikimediaDataRepository wikimediaDataRepository;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaDatabaseConsumer.class);

    @KafkaListener(
            topics = "wikimedia-changes",
            groupId = "myGroup"
    )
    public void consumeMessage(String eventMessage) {
        LOG.info(String.format("#### -> Event message received -> %s", eventMessage));
        WikimediaData wikimediaData = new WikimediaData();
        wikimediaData.setWikiEventData(eventMessage);
        wikimediaDataRepository.save(wikimediaData);
    }
}
