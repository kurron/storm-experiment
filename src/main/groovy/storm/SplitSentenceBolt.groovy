package storm

import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Values
import groovy.util.logging.Slf4j
import backtype.storm.tuple.Tuple as StormTuple

@Slf4j
class SplitSentenceBolt extends BaseRichBolt {

    private OutputCollector theCollector

    @Override
    void declareOutputFields( OutputFieldsDeclarer declarer ) {
        declarer.declare( new Fields( 'word' ) )
    }

    @Override
    void prepare( Map stormConf, TopologyContext context, OutputCollector collector ) {
        theCollector = collector
    }

    @Override
    void execute( StormTuple tuple ) {
        String sentence = tuple.getStringByField( 'sentence' )
        sentence.split().each { word ->
            theCollector.emit (new Values( word ) )
        }
    }
}