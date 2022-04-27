package com.apriltraining.orderProcessing.service;



import com.apriltraining.orderProcessing.models.Order;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProcessorListener {

    private KafkaTemplate<String,String> kafkaTemplate;

    private ObjectMapper objectMapper;


    public OrderProcessorListener(@Autowired KafkaTemplate<String,String> kafkaTemplate, @Autowired ObjectMapper objectMapper){
        this.kafkaTemplate= kafkaTemplate;
        this.objectMapper =objectMapper;
    }


    @KafkaListener(topics = "topic-1",groupId = "custom_group")
    public void listenGroupFoo(String message) throws JsonProcessingException {
        System.out.println("Received message in order place service: " + message);

        Order order = objectMapper.readValue(message,Order.class);

        order.setStatus("ORDER_PROCESSED");

        kafkaTemplate.send("topic-2",objectMapper.writeValueAsString(order));
    }
}
