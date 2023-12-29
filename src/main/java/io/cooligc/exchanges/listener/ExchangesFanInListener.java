package io.cooligc.exchanges.listener;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Component
public class ExchangesFanInListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExchangesFanInListener.class);

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Value("${kafka.topic.registration}")
    private String registrationTopic;

    @KafkaListener(topics = "${kafka.topic.fan-in}")
    public void doListen(@Payload String messages, @Headers Map<String,String> headerMap){
        
        LOGGER.info("Message Received at Fan-In Services messages = {} , headers = {} ", messages , headerMap);


        Map<String,Object> headers = new HashMap<>();
        headers.put("x-user-id", headerMap.get("x-user-id").toString());
        headers.put("x-transaction-id", headerMap.get("x-transaction-id").toString());
        headers.put("x-corelation-id", headerMap.get("x-corelation-id").toString());
        headers.put(KafkaHeaders.TOPIC,registrationTopic);
        headers.put(KafkaHeaders.MESSAGE_KEY, UUID.randomUUID().toString());


        Message<String> _message = MessageBuilder.createMessage(messages, new MessageHeaders(headers));
        kafkaTemplate.send(_message);
    }


}
