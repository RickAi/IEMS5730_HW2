package wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.*;

public class WordCountTopology {

    public static class FileReaderSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;
        private boolean processed = false;

        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.collector = spoutOutputCollector;
        }

        @Override
        public void nextTuple() {
            if (processed) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                BufferedReader reader = null;
                try {
                    reader = new BufferedReader(new FileReader(DATA_PATH));
                    String buffer;
                    while ((buffer = reader.readLine()) != null) {
                        this.collector.emit(new Values(buffer));
                        counter(CounterType.EMIT);
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    // make sure the file only processed once
                    processed = true;
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("line"));
        }
    }

    public static class SplitSentenceBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            String line = tuple.getStringByField("line");
            // split with regex, only include alphabet code
            String[] words = line.split("\\W+");
            for (String word : words) {
                if (!word.isEmpty()) {
                    this.collector.emit(new Values(word));
                    counter(CounterType.EMIT);
                }
            }
            this.collector.ack(tuple);
            counter(CounterType.ACK);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }

    public static class WordCountBolt extends BaseRichBolt {

        private OutputCollector collector;
        private LinkedHashMap<String, Integer> counterMap;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
            this.counterMap = new LinkedHashMap<String, Integer>();
        }

        @Override
        public void execute(Tuple tuple) {
            String word = tuple.getStringByField("word");
            if (counterMap.containsKey(word)) {
                counterMap.put(word, counterMap.get(word) + 1);
            } else {
                counterMap.put(word, 1);
            }
            this.collector.ack(tuple);
            counter(CounterType.ACK);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}

        @Override
        public void cleanup() {
            // dump counters into the console
            dumpCounters();

            System.out.println("cleanup, sortByValue counterMap start");
            // sort and save result into local file
            Utils.sortByValue(counterMap, new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o2.compareTo(o1);
                }
            });

            System.out.println("cleanup, start to save counterMap into file");
            FileWriter fw = null;
            BufferedWriter writer = null;
            try {
                fw = new FileWriter(RESULT_PATH);
                writer = new BufferedWriter(fw);
                for (Map.Entry<String, Integer> entry : counterMap.entrySet()) {
                    writer.write(entry.getKey() + "\t" + String.valueOf(entry.getValue()));
                    writer.newLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (writer != null) {
                        writer.close();
                    }
                    if (fw != null) {
                        fw.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("cleanup, end save counterMap into file");
        }
    }

    // Counter Code START
    public static int emit_counter = 0;
    public static int ack_counter = 0;

    public static enum CounterType {EMIT, ACK};

    public static synchronized void counter(CounterType type) {
        if (type == CounterType.EMIT) {
            emit_counter += 1;
        } else if (type == CounterType.ACK) {
            ack_counter += 1;
        }
    }

    public static void dumpCounters() {
        System.out.println("--------DUMP COUNTERS START--------");
        System.out.println("The number of tuple emitted:" + emit_counter);
        System.out.println("The number of tuple acked:" + ack_counter);
        System.out.println("The number of tuple failed:" + (emit_counter - ack_counter));
        System.out.println("--------DUMP COUNTERS END--------");
    }
    // Counter Code END

    public static final String ID_FILE_READ_SPOUT = "file-reader-spout";
    public static final String ID_SPLIT_BOLT = "split-bolt";
    public static final String ID_COUNT_BOLT = "count-bolt";

    public static final String DATA_PATH = "StormData.txt";
    public static final String RESULT_PATH = "wordcount_result.txt";

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        // this topology is designed work on a single machine
        // since file read in spout is hard to achieve parallelism
        builder.setSpout(ID_FILE_READ_SPOUT, new FileReaderSpout(), 1);
        builder.setBolt(ID_SPLIT_BOLT, new SplitSentenceBolt(), 8).shuffleGrouping(ID_FILE_READ_SPOUT);
        builder.setBolt(ID_COUNT_BOLT, new WordCountBolt(), 1).shuffleGrouping(ID_SPLIT_BOLT);

        Config conf = new Config();
        conf.setDebug(true);

        try {
            if (args != null && args.length > 0) {
                conf.setNumWorkers(1);
                StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
            } else {
                conf.setMaxTaskParallelism(3);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("word-count", conf, builder.createTopology());

                // local debug cluster only process 1 hour
                // make sure the time is long enough, the debug user have to stop manually
                Thread.sleep(60 * 1000 * 60);

                cluster.shutdown();
            }
        } catch (Exception e) {
            System.out.println("submit failed with error:" + e.toString());
        }
    }

}
