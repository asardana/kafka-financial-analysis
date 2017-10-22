package com.kafka.financial;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Created by Aman on 10/21/2017.
 */
public class LoanDataKafkaProducer {

    private static String kafkaBrokerEndpoint = null;
    private static String loanDataStatsInputFile = null;
    private static String loanDataIngestTopic = null;

    public static void main(String args[]) {

        if (args != null) {

            // Read command Line Arguments for Kafka broker, Topic for publishing the Loan Records and the input file for reading the loan records
            kafkaBrokerEndpoint = args[0];
            loanDataIngestTopic = args[1];
            loanDataStatsInputFile = args[2];
        }

        LoanDataKafkaProducer kafkaProducer = new LoanDataKafkaProducer();

        // Publish the Loan records
        kafkaProducer.publishLoanStatFinancialData();
    }

    /**
     * Publish the Loan Data Stats to the Kafka Broker
     *
     */
    private void publishLoanStatFinancialData() {

        final Producer<String, String> loanDataProducer = createKafkaProducer();

        // Latch to make sure all the records are published asynchronously using the callback mechanism
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        try {

            // Read the input file as a stream of lines
            Stream<String> loanDataFileStream = Files.lines(Paths.get(loanDataStatsInputFile));

            // Convert each line of record to a Kafka Producer Record
            // Generate a Random UUID for the key. Value is line of loan record in JSON format
            loanDataFileStream.forEach(line -> {

                final ProducerRecord<String, String> loanRecord =
                        new ProducerRecord<String, String>(loanDataIngestTopic, UUID.randomUUID().toString(), line);

                // Send the loan record to Kafka Broker in Async mode. Callback is called after the record receiving the acknowledgement from broker
                loanDataProducer.send(loanRecord, ((metadata, exception) -> {

                    if (metadata != null) {
                        System.out.println("Loan Data Event Sent --> " + loanRecord.key() + " | "
                                + loanRecord.value() + " | " + metadata.partition());
                    } else {

                        System.out.println("Error Sending Loan Data Event --> " + loanRecord.value());
                    }
                }));
            });
            try {

                // Wait for 20 seconds to get the records processed before proceeding further with the processing
                countDownLatch.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Creates the Kafka Producer with the required configuration
     * @return Producer
     */
    private Producer<String, String> createKafkaProducer() {

        Properties prop = new Properties();

        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerEndpoint);
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, "LoanDataKafkaProducer");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, String>(prop);
    }
}
