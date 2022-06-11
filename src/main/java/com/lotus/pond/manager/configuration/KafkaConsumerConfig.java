package com.lotus.pond.manager.configuration;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    Logger logger = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Value(value = "${bootstrap.servers}")
    private String bootstrapAddress;

    @Value(value = "${sasl.jaas.config}")
    private String saslJaasConfig;

    @Value(value = "${security.protocol}")
    private String securityProtocol;

    @Value(value = "${sasl.mechanism}")
    private String saslMechanism;

    @Value(value = "${consumer.client-id}")
    private String clientId;

    @Value(value = "${consumer.group-id}")
    private String groupId;

    @Bean
    public ConsumerFactory<Integer,String> consumerFactory(){
        logger.info("config consumer ");
        Map<String, Object> configProps = configs();
        DefaultKafkaConsumerFactory<Integer, String> result = new DefaultKafkaConsumerFactory<>(configProps);
        logger.info("result [{}]", result.getConfigurationProperties());
        return result;
    }

    Map<String, Object> configs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configs.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
        configs.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        configs.put("security.protocol", securityProtocol);
        return configs;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}
