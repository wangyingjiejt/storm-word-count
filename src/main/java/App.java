import bolts.WordCounter;
import bolts.WordNormalizer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import spout.WordReader;

public class App {
    public static void main(String[] args) throws InterruptedException {
        //定义topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader",new WordReader());
        builder.setBolt("word-normalizer",new WordNormalizer()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter",new WordCounter(),2).fieldsGrouping("word-normalizer",new Fields("word"));


        //配置
        Config conf = new Config();
        conf.put("wordsFile","src/main/resources/words.txt");
        conf.setDebug(true);


        //运行topology
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING,1);
        LocalCluster cluster= new LocalCluster();
        cluster.submitTopology("Getting-start",conf,builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}
