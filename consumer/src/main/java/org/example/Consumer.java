package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    public static final Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

    public static void main(String[] args) {
        // consumer setting
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-statefulset-0.kafka-service:29092,kafka-statefulset-1.kafka-service:29092,kafka-statefulset-2.kafka-service:29092");
//        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "mlairport1");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String topicName = "ml-airport";


        // consumer 생성
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // topic 설정
        consumer.subscribe(Collections.singletonList(topicName));
        PreparedStatement preparedStatement = null;
        try {

            DBConnect dbConnect = new PostgresConnect();
            Connection connection = dbConnect.getConnection();
            String insertQuery = "INSERT INTO airport (" +
                    "Year, Month, DayofMonth, DayOfWeek, DepTime, CRSDepTime, ArrTime, " +
                    "CRSArrTime, UniqueCarrier, FlightNum, TailNum, ActualElapsedTime, " +
                    "CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin, Dest, Distance, " +
                    "TaxiIn, TaxiOut, Cancelled, CancellationCode, Diverted, CarrierDelay, " +
                    "WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay, insert_dt) " +
                    "VALUES (" +
                    "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            preparedStatement = connection.prepareStatement(insertQuery);
            int i = 0;
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records){

                    String messageValue = (String)record.value();

                    String[] tokens = messageValue.split(",");
                    AirportDTO airportDTO = new AirportDTO(tokens[0], tokens[1], tokens[2], tokens[3],
                            tokens[4], tokens[5], tokens[6], tokens[7], tokens[8], tokens[9], tokens[10], tokens[11], tokens[12], tokens[13],tokens[14],
                            tokens[15],tokens[16],tokens[17],tokens[18],tokens[19],tokens[20],tokens[21],tokens[22],tokens[23],tokens[24],tokens[25],tokens[26],tokens[27],
                            tokens[28], LocalDateTime.now()
                    );
                    i++;

                    logger.info("{} , airportDTO => {}", i, airportDTO);


                    preparedStatement.setString(1, airportDTO.Year);
                    preparedStatement.setString(2, airportDTO.Month);
                    preparedStatement.setString(3, airportDTO.DayofMonth);
                    preparedStatement.setString(4, airportDTO.DayOfWeek);
                    preparedStatement.setString(5, airportDTO.DepTime);
                    preparedStatement.setString(6, airportDTO.CRSDepTime);
                    preparedStatement.setString(7, airportDTO.ArrTime);
                    preparedStatement.setString(8, airportDTO.CRSArrTime);
                    preparedStatement.setString(9, airportDTO.UniqueCarrier);
                    preparedStatement.setString(10, airportDTO.FlightNum);
                    preparedStatement.setString(11, airportDTO.TailNum);
                    preparedStatement.setString(12, airportDTO.ActualElapsedTime);
                    preparedStatement.setString(13, airportDTO.CRSElapsedTime );
                    preparedStatement.setString(14, airportDTO.AirTime);
                    preparedStatement.setString(15, airportDTO.ArrDelay);
                    preparedStatement.setString(16, airportDTO.DepDelay);
                    preparedStatement.setString(17, airportDTO.Origin);
                    preparedStatement.setString(18, airportDTO.Dest);
                    preparedStatement.setString(19, airportDTO.Distance);
                    preparedStatement.setString(20, airportDTO.TaxiIn);
                    preparedStatement.setString(21, airportDTO.TaxiOut);
                    preparedStatement.setString(22, airportDTO.Cancelled);
                    preparedStatement.setString(23, airportDTO.CancellationCode);
                    preparedStatement.setString(24, airportDTO.Diverted);
                    preparedStatement.setString(25, airportDTO.CarrierDelay);
                    preparedStatement.setString(26, airportDTO.WeatherDelay);
                    preparedStatement.setString(27, airportDTO.NASDelay);
                    preparedStatement.setString(28, airportDTO.SecurityDelay);
                    preparedStatement.setString(29, airportDTO.LateAircraftDelay);
                    preparedStatement.setTimestamp(30, Timestamp.valueOf(airportDTO.insert_dt));   // column2에 Kafka 메시지 값을 매핑
                    preparedStatement.addBatch();


                }

                preparedStatement.executeBatch();

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            consumer.close();
        }

    }
}
