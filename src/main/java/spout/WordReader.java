package spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

/**
 * 按行读取文件并每一行发布一个元组
 * @user wangyj
 * @date 2018/1/30 15:33
 */
public class WordReader implements IRichSpout {
    private SpoutOutputCollector collector;

    private FileReader fileReader;

    private boolean completed =false;

    private TopologyContext context;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        try{
            this.context=topologyContext;
            this.fileReader=new FileReader(map.get("wordsFile").toString());
            this.collector=spoutOutputCollector;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("Error reading wordsFile");
        }
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    /**
     * 该方法用于分发文本中的文本行
     * @user wangyj
     * @date 2018/1/30 15:14
     */
    public void nextTuple() {
        if (completed){
            try {
                //分发完成后，休眠
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return;
        }

        String str;
        BufferedReader reader =new BufferedReader(fileReader);
        try {
            //读取所有文本
            while ((str=reader.readLine())!=null){
                this.collector.emit(new Values(str),str);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            completed=true;
        }
    }

    public void ack(Object o) {

    }

    public void fail(Object o) {

        System.out.println("failed");
    }


    /**
     * 声明输入域“word"
     * @user wangyj
     * @date 2018/1/30 15:29
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
