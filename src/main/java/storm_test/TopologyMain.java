package storm_test;

import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import storm_test.bolts.SentenceBolt;
import storm_test.bolts.WordCounter;
import storm_test.bolts.WordNormalizer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dytwest on 2017/4/10.
 */
public class TopologyMain {

    public static void main(String[] args) throws InterruptedException {

        //Configure kafkaSpout
        BrokerHosts brokerHosts = new ZkHosts("localhost:2181");
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "my-topic", "/topology/root", "wordCount");
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader",new KafkaSpout(spoutConfig));
        builder.setBolt("sentence-reader", new SentenceBolt()).shuffleGrouping("word-reader");
        builder.setBolt("word-normalizer", new WordNormalizer())
                .shuffleGrouping("sentence-reader");
        builder.setBolt("word-counter", new WordCounter(),1)
                .fieldsGrouping("word-normalizer", new Fields("word"));

        //Configuration
        Config config = new Config();
        Map<String, String> map = new HashMap<>();
        map.put("metadata.broker.list", "localhost:9092");
        map.put("serializer.class", "kafka.serializer.StringEncoder");
        config.put("kafka.broker.properties", map);
        config.put("topic", "my-topic");
        config.setDebug(false);

        //Topology run
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Toplogie", config, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}
