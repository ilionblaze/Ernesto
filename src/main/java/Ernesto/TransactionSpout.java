package main.java.Ernesto;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.TestWordSpout;
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
                "localhost",
                5672,
                "guest",
                "guest",
                "/",
                new SharedQueueWithBinding("hello", "test", "#"),
                new SimpleJSONScheme()
        ));

        //builder.setSpout("words", new TestWordSpout(), 1);

        //builder.setBolt("mongo", new SimpleMongoBolt("10.100.20.150", 27017, "storm", "things")).shuffleGrouping("words");
        builder.setBolt("mongo", new TransactionBolt("localhost", 27017, "storm", "things")).shuffleGrouping("Spout");

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("test", config, builder.createTopology());

        Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();

    }

}
