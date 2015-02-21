package storm

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Values
import groovy.util.logging.Slf4j

import static storm.FieldNames.SENTENCE

/**
 * Spouts are the source of the stream.  In a real application, this would be an integration with a queue,
 * database, file, REST call, etc.  It also produces a new stream and emits it.
 */
@Slf4j
class SentenceSpout extends BaseRichSpout {

    private SpoutOutputCollector theCollector
    private int index = 0

    @Override
    void nextTuple() {
        // we've been told to send another data point to the stream via the collector
        def messageID = UUID.randomUUID().toString()
        theCollector.emit( new Values( sentences[index] ), messageID )
        index++
        if ( index >= sentences.size() ) {
            index = 0
        }
    }

    @Override
    void ack( Object msgId ) {
        log.debug( 'ack called' )
    }

    @Override
    void fail( Object msgId ) {
        log.error( 'fail called with {}', msgId )
    }

    @Override
    void declareOutputFields( OutputFieldsDeclarer declarer ) {
        // tell the rest of the world what we will be emitting
        declarer.declare( new Fields( SENTENCE.name() ) )
    }

    @Override
    void open( Map conf, TopologyContext context, SpoutOutputCollector collector ) {
        // when the system is initialized, save the collector s we can send values to it
        theCollector = collector
    }


    def sentences = ["Two households, both alike in dignity",
                     "In fair Verona, where we lay our scene",
                     "From ancient grudge break to new mutiny",
                     "Where civil blood makes civil hands unclean.",
                     "From forth the fatal loins of these two foes",
                     "A pair of star-cross'd lovers take their life",
                     "Whose misadventured piteous overthrows",
                     "Do with their death bury their parents' strife.",
                     "The fearful passage of their death-mark'd love",
                     "And the continuance of their parents' rage",
                     "Which, but their children's end, nought could remove",
                     "Is now the two hours' traffic of our stage",
                     "The which if you with patient ears attend",
                     "What here shall miss, our toil shall strive to mend."]
}

