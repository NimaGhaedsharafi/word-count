package storm.starter;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class StatisticsBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(StatisticsBolt.class);

    private Map<String, Long> counter;
    private long lastLogTime;
    private long lastClearTime;

    /** Number of seconds before the top list will be logged to stdout. */
    private final long logIntervalSec;

    /** Number of seconds before the top list will be cleared. */
    private final long clearIntervalSec;

    /** Number of top words to store in stats. */
    private final int topListSize;

    public StatisticsBolt(long logIntervalSec, long clearIntervalSec, int topListSize) {
        this.logIntervalSec = logIntervalSec;
        this.clearIntervalSec = clearIntervalSec;
        this.topListSize = topListSize;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        counter = new HashMap<>();
        lastLogTime = System.currentTimeMillis();
        lastClearTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = (String) tuple.getValueByField("word");
        Long count = this.counter.get(word);
        count = count == null ? 1L : count + 1;
        this.counter.put(word, count);


        long logPeriodSec = (System.currentTimeMillis() - this.lastLogTime) / 1000;
        if (logPeriodSec > this.logIntervalSec) {
            logger.info("\n*******\n");
            logger.info("Word count: " + this.counter.size());
            logger.info("\n*******\n");

            lastLogTime = System.currentTimeMillis();
        }
    }

    private void showTopWords() {
        // calculate top list:
        SortedMap<Long, String> top = new TreeMap<>();
        for (Map.Entry<String, Long> entry : this.counter.entrySet()) {
            long count = entry.getValue();
            String word = entry.getKey();

            top.put(count, word);
            if (top.size() > topListSize) {
                top.remove(top.firstKey());
            }
        }

        // Output top list:
        for (Map.Entry<Long, String> entry : top.entrySet()) {
            logger.info("top - " + entry.getValue() + '|' + entry.getKey());
        }

        // Clear top list
        long now = System.currentTimeMillis();
        if (now - lastClearTime > clearIntervalSec * 1000) {
            counter.clear();
            lastClearTime = now;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
