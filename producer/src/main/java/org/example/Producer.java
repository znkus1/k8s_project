package org.example;

// kafka import
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

// logger import
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// property import
import java.util.Properties;

// file reader import

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Producer {
    private final static Logger logger = LoggerFactory.getLogger(Producer.class);
    private final static String TOPIC_NAME = "ml-airport";
    private final static String BOOTSTRAP_SERVERS = "kafka-statefulset-0.kafka-service:29092,kafka-statefulset-1.kafka-service:29092,kafka-statefulset-2.kafka-service:29092";

    public static void main(String[] args) {
        // proucer config setting

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // Idempotent 설정 (enable.idempotence=true)
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // Retry 설정 (retries=3)
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // Retry 간격 설정 (retry.backoff.ms=100)
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // producer object create
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // message key-value setting
        String messageKey = null;


        Path path = Paths.get("");
        String separator = File.separator;
        String dirPath = path.toAbsolutePath().toString() + separator +"resources";
//        String dirPath =  "C:\\Users\\82103\\Downloads\\1987";
        File directory = new File(dirPath);

        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles();

            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {
                        // 파일 목록 출력 또는 원하는 작업 수행
                        logger.info("File => {} " , file.getName());


                        String line = "";
                        int i =0;
                        try {
                            FileReader fileReader = new FileReader(dirPath+separator+file.getName());
                            BufferedReader bufferedReader= new BufferedReader(fileReader);

                            while( (line = bufferedReader.readLine()) != null){
                                i++;
                                if(i ==1){
                                    continue;
                                }
                                logger.info("i = {} record => {}",i,line);
                                String messageValue = line;

                                // producer record create
                                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageKey, messageValue);

                                // producer send async
                                producer.send(record, (metadata, exception) -> {
                                    if (exception == null){
                                        logger.info(
                                                "topic => "+ metadata.topic()+ "\n"+
                                                        "partition => "+ metadata.partition() + "\n"+
                                                        "offset => "+ metadata.offset()
                                        );
                                    }
                                    else{
                                        logger.error("exception => ", exception);
                                    }
                                });

                            }
                        }catch (IOException e){
                            logger.info(e.getMessage());
                        }





                    }
                }
            }
        } else {
            logger.info("Directory does not exist or is not a directory.");
        }


        // producer close
        producer.close();


    }
}