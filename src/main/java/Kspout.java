import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import java.util.Map;
import java.util.Properties;

public class Kspout extends BaseRichBolt {
    Producer<String,String> producer;
    Properties props;



    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.producer =producer;
        props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaSpoutConfig<String, String> spoutConfig = KafkaSpoutConfig.builder(
                        "localhost:9092", "Receive")
                .setProp(props)
                .setOffsetCommitPeriodMs(100).setGroupId("User_group")
                .build();
        producer = new KafkaProducer<>(props);

    }


    @Override
    public void execute(Tuple tuple) {
        User user = (User) tuple.getValue(0);
        ProducerRecord<String,String> record = new ProducerRecord<String,String>("Receive","id , name , email , age",(user.getId()+", "+user.getName()+", "+user.getEmail()+", "+user.getAge()));
        producer.send(record);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        producer.close();;
    }
}