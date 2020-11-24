package com.padma.trials.application.controllers;

import io.swagger.annotations.ApiOperation;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.inject.Inject;
import javax.inject.Named;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    private Producer kafkaProducer;
    private Consumer kafkaConsumer;
    private Properties streamsConfig;

    @Inject
    public KafkaController(@Named("KafkaProducer") final Producer kafkaProducer,
                           @Named("KafkaConsumer") final Consumer kafkaConsumer,
                           @Named("StreamsConfig") final Properties streamsConfig){
        this.kafkaProducer = kafkaProducer;
        this.kafkaConsumer = kafkaConsumer;
        this.streamsConfig = streamsConfig;
    }

    @RequestMapping(value = "/produce", method = RequestMethod.POST)
    @ApiOperation(value = "Producer", response = String.class)
    public String produce(@RequestParam String message) throws ExecutionException, InterruptedException {
        ProducerRecord record = new ProducerRecord("MyTestTopic", message);
        RecordMetadata metadata = (RecordMetadata) kafkaProducer.send(record).get();
        return String.format("Partition: %d & Offset : %d", metadata.partition(), metadata.offset());
    }

    @RequestMapping(value = "/consume", method = RequestMethod.GET)
    @ApiOperation(value = "Consumer", response = String.class)
    public List<String> consume(){
        int retryCount = 0;
        while(true){
            ConsumerRecords consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(10));
            if(consumerRecords.isEmpty()){
                retryCount++;
                if(retryCount == 100)
                    return new ArrayList<>();
                else
                    continue;
            }
            List<String> messages = new ArrayList<>();
            consumerRecords.forEach(record ->{
                ConsumerRecord consumerRecord = (ConsumerRecord) record;
                messages.add(String.format("Key : %s ; Value %s ; Partition: %d ; Offset: %d "
                        ,consumerRecord.key(), consumerRecord.value(), consumerRecord.partition(), consumerRecord.offset()));
            });
            kafkaConsumer.commitAsync();
            return messages;
        }
    }

    public Topology getStreamsTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream("MyTestTopic");
        Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
        KTable<String, Long> wordCounts = input
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .groupBy((key, word) -> word)
                .count();
        wordCounts.toStream().to("MyStreamsOutput", Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    @RequestMapping(value = "/initStreamProcessing", method = RequestMethod.GET)
    @ApiOperation(value = "Initialise Stream Processing", response = String.class)
    public void initStreamProcessing(){
        Properties properties = streamsConfig;
        Topology streamsTopology = getStreamsTopology();
        KafkaStreams streams = new KafkaStreams(streamsTopology, properties);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

