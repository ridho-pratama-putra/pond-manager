package com.lotus.pond.manager;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class PondListener {

    Logger logger = LoggerFactory.getLogger(PondListener.class);

    @KafkaListener(topics = {"pond"})
    public void listener(String value) {
        logger.info("I got [{}]", value);
    }
}
