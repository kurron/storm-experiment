package storm

import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import groovy.util.logging.Slf4j
import backtype.storm.tuple.Tuple as StormTuple

@Slf4j
class PrinterBolt extends BaseRichBolt {

    Map<String,Long> theCounts = [:]

    @Override
    void declareOutputFields( OutputFieldsDeclarer declarer ) {
    }

    @Override
    void prepare( Map stormConf, TopologyContext context, OutputCollector collector) {
        theCounts = [:]
    }

    @Override
    void execute( StormTuple tuple ) {
        String word = tuple.getStringByField( 'word' )
        Long count = tuple.getLongByField( 'count' )
        theCounts.put( word, count )
    }

    @Override
    void cleanup() {
        log.info( 'final count' )
        theCounts.sort { it.value }.reverseEach { k, v ->
            log.info( "'$k' - $v" )
        }
    }
}
