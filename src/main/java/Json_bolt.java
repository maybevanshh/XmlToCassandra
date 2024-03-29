import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class Json_bolt extends BaseRichBolt {
    OutputCollector outputCollector;
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        User user = new User();
        Map<String,Object> map = (Map<String, Object>) tuple.getValue(0);
        Set<String> set = map.keySet();
            set.stream().forEach(str -> {
                if (str.toString().equals("name")){
                    user.setName((String) map.get(str));
                }
                if (str.toString().equals("email")){
                    user.setEmail((String) map.get(str));
                }
                if (str.toString().equals("age")){
                    user.setAge((Integer) map.get(str));
                }
                if (str.toString().equals("id")){
                    user.setId((Integer) map.get(str));
                }
            });
        outputCollector.emit(Collections.singletonList(user));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Users"));

    }
}
