package twitter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TwitterHashTagTopology {

    // 10 mins for print report
    public static final int TIME_INTERVAL_PRINT = 10 * 60;

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        // key, secret, token and secret from query tweets from API
        TweetSpout tweetSpout = new TweetSpout(
                "uIf2XoMNpF08egIeVSF4cXBfT",
                "YXU76uvy7BJTzjxunCR0iYnEnWOBHadcAAHNVTINaXqiITesjt",
                "804585635767930880-0n6IHZ0XSQfOgel14oeG93spT8ObkZn",
                "xl4ft5od8Kks17FakZv30N3VR0LSJmmG2uqCOAtfZ75j3"
        );
        builder.setSpout("tweet-spout", tweetSpout, 1);
        // extract tags from tweet
        builder.setBolt("hashtag-bolt", new HashTagBolt(), 10).shuffleGrouping("tweet-spout");
        // only emit in every time slot (10 times as required)
        builder.setBolt("rolling-count-bolt", new RollingCountBolt(TIME_INTERVAL_PRINT * 2, TIME_INTERVAL_PRINT), 1)
                .fieldsGrouping("hashtag-bolt", new Fields("type", "hashtag"));
        // report the popular hash tags
        builder.setBolt("reporter-bolt", new ReporterBolt(), 1).globalGrouping("rolling-count-bolt");

        Config conf = new Config();
        // turn this on if debug is needed
        conf.setDebug(false);

        try {
            if (args != null && args.length > 0) {
                conf.setNumWorkers(1);
                StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
            } else {
                conf.setMaxTaskParallelism(3);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("twitter-hashtag", conf, builder.createTopology());

                // local debug, sleep 10s
                Thread.sleep(60 * 1000);

                cluster.shutdown();
            }
        } catch (Exception e) {
            System.out.println("submit failed with error:" + e.toString());
        }
    }

}
