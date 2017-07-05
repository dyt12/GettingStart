package storm_test.bolts;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by dytwest on 2017/4/18.
 */
public class SentenceBolt extends BaseBasicBolt{

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
//        String msg = tuple.getStringByField("msg");
        String msg = tuple.getString(0);

        basicOutputCollector.emit(new Values(msg));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
