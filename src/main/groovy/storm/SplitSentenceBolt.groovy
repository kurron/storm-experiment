package storm

import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Values
import groovy.util.logging.Slf4j
import backtype.storm.tuple.Tuple as StormTuple

import static storm.FieldNames.SENTENCE
import static storm.FieldNames.WORD

/**
 * Spouts are the source of the stream.  In a real application, this would be an integration with a queue,
 * database, file, REST call, etc.  It also produces a new stream and emits it.
 */
@Slf4j
class SplitSentenceBolt extends BaseRichBolt {

    private OutputCollector theCollector

    @Override
    void declareOutputFields( OutputFieldsDeclarer declarer ) {
        declarer.declare( new Fields( WORD.name() ) )
    }

    @Override
    void prepare( Map stormConf, TopologyContext context, OutputCollector collector ) {
        theCollector = collector
    }

    @Override
    void execute( StormTuple tuple ) {
        String sentence = tuple.getStringByField( SENTENCE.name() )
        sentence.split().each { word ->
            theCollector.emit( new Values( word ) )
        }
    }
}