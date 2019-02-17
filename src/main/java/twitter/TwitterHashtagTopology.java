package twitter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class TwitterHashtagTopology {



    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        Config conf = new Config();
        conf.setDebug(true);

        try {
            if (args != null && args.length > 0) {
                conf.setNumWorkers(1);
                StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
            } else {
                conf.setMaxTaskParallelism(3);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("twitter-hashtag", conf, builder.createTopology());

                // local debug, sleep 10s
                Thread.sleep(10 * 1000);

                cluster.shutdown();
            }
        } catch (Exception e) {
            System.out.println("submit failed with error:" + e.toString());
        }
    }

}
