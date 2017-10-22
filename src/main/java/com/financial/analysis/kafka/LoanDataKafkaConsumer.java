package com.financial.analysis.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by Aman on 10/21/2017.
 */
public class LoanDataKafkaConsumer {

    private static String kafkaBrokerEndpoint = null;
    private static String loanDataIngestTopic = null;

    public static void main(String args[]){

        // Read command Line Arguments for Kafka broker and Topic for publishing the Loan Records
        if (args != null) {

            kafkaBrokerEndpoint = args[0];
            loanDataIngestTopic = args[1];
        }

        LoanDataKafkaConsumer loanDataKafkaConsumer = new LoanDataKafkaConsumer();

        loanDataKafkaConsumer.consumeLoanStatFinancialData();
    }

    /**
     * Consumes the Loan Data Records from the Kafka Broker
     */
    private void consumeLoanStatFinancialData(){

        final Consumer<String, String> loanDataConsumer = createKafkaConsumer();

        while(true){

            // Kafka Consumer polls the Topic every 5 seconds to get the new messages
            final ConsumerRecords<String, String> loanRecords = loanDataConsumer.poll(5000);

            loanRecords.forEach(loanRecord -> {
                System.out.println("Received Record --> " + loanRecord.key() + " | " + loanRecord.offset() + " | "
                        + loanRecord.partition() + " | " + loanRecord.value());
            });
            loanDataConsumer.commitAsync();
        }
    }

    /**
     * Creates the Kafka Consumer with the required cofiguration
     * @return KafkaConsumer
     */
    private KafkaConsumer<String, String> createKafkaConsumer(){

        Properties prop = new Properties();

        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerEndpoint);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "LoanDataKafkaConsumer");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        final KafkaConsumer kafkaConsumer = new KafkaConsumer<String, String>(prop);
        kafkaConsumer.subscribe(Collections.singletonList(loanDataIngestTopic));

        return kafkaConsumer;
    }
}
