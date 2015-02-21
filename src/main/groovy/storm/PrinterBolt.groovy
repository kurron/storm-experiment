package storm

import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import groovy.util.logging.Slf4j

@Slf4j
class PrinterBolt extends BaseRichBolt {

    HashMap counts = null

    @Override
    void declareOutputFields( OutputFieldsDeclarer declarer ) {
    }

    @Override
    void prepare( Map stormConf, TopologyContext context, OutputCollector collector) {
        this.counts = [:]
    }

    @Override
    void execute( backtype.storm.tuple.Tuple tuple ) {
        String word = tuple.getStringByField( 'word' )
        Long count = tuple.getLongByField( 'count' )
        counts.put( word, count )
    }

    @Override
    void cleanup() {
        log.info( 'final count' )
        counts.sort { it.value }.reverseEach { k, v ->
            log.info( "'$k' - $v" )
        }
    }
}
