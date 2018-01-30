package bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
/**
 * 对每行的进行单词分割  处理
 * @user wangyj
 * @date 2018/1/30 15:33
 */
public class WordNormalizer implements IRichBolt {
    private OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector;
    }

    public void execute(Tuple tuple) {

        String line= tuple.getString(0);
        String[] words= line.split(" ");
        for (String word:words){
            word=word.trim();

            if (word.length()>0){
                word=word.toLowerCase();

                //发布这个单词
                List a= new ArrayList();
                a.add(tuple);
                collector.emit(a,new Values(word));
            }
        }

        //确认元组
        collector.ack(tuple);
    }

    public void cleanup() {

    }

    /**
     * 这个bolt只会发布”word"域
     * @user wangyj
     * @date 2018/1/30 15:43
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
