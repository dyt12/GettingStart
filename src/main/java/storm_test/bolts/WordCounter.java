package storm_test.bolts;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dytwest on 2017/4/10.
 */
public class WordCounter extends BaseBasicBolt {

    Integer id;
    String name;
    Map<String, Integer> counters;

    /**
     * At the end of the spout (when the cluster is shutdown
     * We will show the word counters
     */
    @Override
    public void cleanup() {
        Jedis jedis = new Jedis("localhost");
        System.out.println("server is running" + jedis.ping());
        System.out.println("-- Word Counter ["+name+"-"+id+"] --");
        for(Map.Entry<String, Integer> entry : counters.entrySet()){
            jedis.set(entry.getKey(), entry.getValue().toString());
            System.out.println(entry.getKey()+": "+entry.getValue());
        }
    }

    /**
     * On create
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.counters = new HashMap<String, Integer>();
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}


    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String str = input.getString(0);
        /**
         * If the word dosn't exist in the map we will create
         * this, if not We will add 1
         */
        if(!counters.containsKey(str)){
            counters.put(str, 1);
        }else{
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }
    }
}
