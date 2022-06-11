package com.lotus.pond.manager;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class PondListener {

    Logger logger = LoggerFactory.getLogger(PondListener.class);

    @KafkaListener(topics = {"pond"})
    public void listener(
            @Payload String value,
            @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) Integer key,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        logger.info("I got [{}] with key [{}] in [{}]", value, key, partition);
    }
}
