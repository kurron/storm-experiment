package storm

import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Values
import groovy.util.logging.Slf4j
import backtype.storm.tuple.Tuple as StormTuple

import static storm.FieldNames.COUNT
import static storm.FieldNames.WORD

/**
 * Bolts are processing units.  The process tuples and perform actions, such as calculation, API calls,
 * database calls, etc.
 */
@Slf4j
class WordCountBolt extends BaseRichBolt {

    private Map<String,Long> theCounts = [:]
    private OutputCollector theCollector

    @Override
    void declareOutputFields( OutputFieldsDeclarer declarer ) {
        declarer.declare( new Fields( WORD.name(), COUNT.name() ) )
    }
    @Override
    void prepare( Map stormConf, TopologyContext context, OutputCollector collector ) {
        theCollector = collector
        theCounts = [:]
    }

    @Override
    void execute( StormTuple tuple ) {
        String word = tuple.getStringByField( WORD.name() )
        Long count = theCounts.get( word )
        if ( count == null ) {
            count = 0L
        }
        count++
        theCounts[word] = count
        theCollector.emit( new Values( word, count ) )
    }
}