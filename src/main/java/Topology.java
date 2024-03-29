import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class Topology {
    public static void main(String[] args) throws Exception {
        JSON_work jsonWork = new JSON_work();


        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("json-spout",jsonWork,1);
        Json_bolt jsonBolt = new Json_bolt();
        topologyBuilder.setBolt("json-bolt",jsonBolt,2).shuffleGrouping("json-spout").setDebug(true);

        topologyBuilder.setBolt("User-bolt",new User_bolt(),1).shuffleGrouping("json-bolt");
        Config config = new Config();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("topology",config,topologyBuilder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();

    }
}
