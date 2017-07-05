package kafka_test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by dytwest on 2017/4/17.
 */


public class KafkaProducerTest {

    private final Producer producer;
//    public final static String TOPIC = "TEST-TOPIC";
    private final String sentence = "Storm test are great is an Storm simple application but very powerful really Storm is great";

    private KafkaProducerTest(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

    void produce() throws InterruptedException{
//        for(int i = 0; i < 100; i++)
        producer.send(new ProducerRecord<String, String>("my-topic", sentence));
        Thread.sleep(10000);
    }

    public static void main(String[] args) throws InterruptedException{
        new KafkaProducerTest().produce();
    }
}
