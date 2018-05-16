package storm.starter;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IgnoranceBolt extends BaseRichBolt {

    private OutputCollector collector;

    private Set<String> IGNORE_LIST = new HashSet<>(Arrays.asList("http", "https", "the", "you", "que", "and", "for", "that", "like", "have", "this", "just", "with", "all", "get",
            "about", "can", "was", "not", "your", "but", "are", "one", "what", "out", "when", "get", "lol", "now", "para", "por",
            "want", "will", "know", "good", "from", "las", "don", "people", "got", "why", "con", "time", "would"));

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = (String) tuple.getValueByField("word");
        if (!IGNORE_LIST.contains(word)) {
            collector.emit(new Values(word));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
