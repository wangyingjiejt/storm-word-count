package bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;


/**
 * 进行单词统计
 * @user wangyj
 * @date 2018/1/30 15:55
 */
public class WordCounter implements IRichBolt {

    Integer id;

    String name;

    Map<String ,Integer> counters;

    private OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.counters=new HashMap<String ,Integer>();
        this.collector=outputCollector;
        this.name=topologyContext.getThisComponentId();
        this.id=topologyContext.getThisTaskId();

    }

    public void execute(Tuple tuple) {
        String str =tuple.getString(0);

        if (!counters.containsKey(str)){
            counters.put(str,1);
        }else {
            Integer c =counters.get(str);
            counters.put(str,c);
        }

        //确认元组
        collector.ack(tuple);

    }

    /**
     * 此bolt结束时  显示单词数量
     * @user wangyj
     * @date 2018/1/30 15:44
     */
    public void cleanup() {

        System.out.println("--单词---数量--");
        for (Map.Entry<String ,Integer> entry:counters.entrySet()
             ) {
            System.out.println("--"+entry.getKey()+"  "+entry.getValue()+"--");
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("wrod"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
