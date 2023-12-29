package io.cooligc.exchanges.controller;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(TestController.class);

    @Autowired
    private KafkaTemplate kafkaTemplate;


    @Value("${kafka.topic.fan-in}")
    private String fanInTopicName;
    

    @PutMapping("/fan-in/publish")
    public String pushMessageToFanIn( @RequestBody Map<String,String> message){


        Map<String,Object> headers = new HashMap<>();
        headers.put("x-user-id", "test-user");
        headers.put("x-transaction-id", UUID.randomUUID().toString());
        headers.put("x-corelation-id", UUID.randomUUID().toString());
        headers.put(KafkaHeaders.TOPIC,fanInTopicName);
        headers.put(KafkaHeaders.MESSAGE_KEY, UUID.randomUUID().toString());


        Message<String> _message = MessageBuilder.createMessage(message.toString(), new MessageHeaders(headers));
        kafkaTemplate.send(_message);

        return "success";
    }


}
