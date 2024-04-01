import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import java.util.Map;

public class User_bolt extends BaseRichBolt {
    CassandraFun cassandraFun;
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        cassandraFun = new CassandraFun();
    }

    @Override
    public void execute(Tuple tuple) {
    User user = (User) tuple.getValue(0);
    cassandraFun.addtoCass(user);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
