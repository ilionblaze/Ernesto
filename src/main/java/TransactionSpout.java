import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.rapportive.storm.amqp.SharedQueueWithBinding;
import com.rapportive.storm.scheme.SimpleJSONScheme;
import com.rapportive.storm.spout.AMQPSpout;
import storm.contrib.mongo.SimpleMongoBolt;

/**
 * Created with IntelliJ IDEA.
 * User: Michael
 * Date: 8/7/12
 * Time: 9:39 PM
 * To change this template use File | Settings | File Templates.
 */
public class TransactionSpout {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Spout", new AMQPSpout(
                "10.100.20.150",
                5672,
                "guest",
                "guest",
                "/",
                new SharedQueueWithBinding("hello", "", "#"),
                new SimpleJSONScheme()
        ));

        builder.setBolt("mongo", new SimpleMongoBolt("10.100.20.150", 27017, "storm", "things")).shuffleGrouping("Spout");

        Config config = new Config();

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("test", config, builder.createTopology());

        Utils.sleep(10000);
        cluster.shutdown();
    }

}
