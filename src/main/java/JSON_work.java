import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;

import java.io.File;
import java.io.FileReader;
import java.util.*;

public class JSON_work extends BaseRichSpout {

    List<Map<String,Object>> list;

    SpoutOutputCollector spoutOutputCollector;
    private Boolean emmited = false;

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try {
        File inp = new File(Topology.class.getResource("output.xml").toURI());
        JSONObject xmljson = XML.toJSONObject(new FileReader(inp));
            list = json_manupulator(xmljson);
        } catch (Exception e) {
            System.out.println(e);
        }
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        if (emmited){
            return;
        }
        list.forEach(stringObjectMap -> {
            spoutOutputCollector.emit(Collections.singletonList(stringObjectMap));
        });
        emmited = true;
        }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("Values"));
    }

    public List<Map<String, Object>> json_manupulator(JSONObject jsonObject) throws Exception {
        ArrayList<JSONObject> jsonObjectArrayList = new ArrayList<>();
        Set<String> keyset = jsonObject.keySet();
        keyset.stream().forEach(set -> {
            jsonObjectArrayList.add(jsonObject.getJSONObject(set));
        });
        ArrayList<JSONArray> internalJson = new ArrayList<>();
        Set<Set<String>> internalkeys = new LinkedHashSet<>();
        jsonObjectArrayList.stream().forEach(jsonObject1 -> {
            internalkeys.add(jsonObject1.keySet());
        });
        jsonObjectArrayList.stream().forEach(jsonObject1 -> {
            internalkeys.stream().forEach(set -> {
                set.stream().forEach(str -> {
                    internalJson.add((JSONArray) jsonObject1.get(str));
                });
            });
        });

        ObjectMapper objectMapper = new ObjectMapper();

        List<Map<String,Object>> listfin = objectMapper.readValue(internalJson.toString().substring(1,internalJson.toString().length()-1),List.class);

        return listfin;
//        System.out.println(listfin);
//        listfin.stream().forEach(map -> {
//
//        });
    }
}
