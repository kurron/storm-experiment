package storm

import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields
import spock.lang.Specification

import static storm.FieldNames.WORD

/**
 * A topology is a network of spouts and bolts.  It defines stream processing pipeline as a graph of
 * computational nodes.
 */
class WordCountTest extends Specification {

    final String SENTENCE_SPOUT_ID = 'sentence-spout'
    final String SPLIT_BOLT_ID = 'splitting-bolt'
    final String COUNT_BOLT_ID = 'counting-bolt'
    final String REPORT_BOLT_ID = 'reporting-bolt'
    final String TOPOLOGY_NAME = 'word-count-topology'

    def 'exercise topology'() {

        given: 'a valid topology'
        def topologyBuilder = new TopologyBuilder()
        // spout -> sentence -> count -> report
        topologyBuilder.setSpout( SENTENCE_SPOUT_ID, new SentenceSpout() )
        topologyBuilder.setBolt( SPLIT_BOLT_ID, new SplitSentenceBolt() ).setNumTasks( 2 )
                       .shuffleGrouping( SENTENCE_SPOUT_ID )
        topologyBuilder.setBolt( COUNT_BOLT_ID, new WordCountBolt(), 2 )
                        .fieldsGrouping( SPLIT_BOLT_ID, new Fields( WORD.name() ) )
        topologyBuilder.setBolt( REPORT_BOLT_ID, new PrinterBolt())
                       .globalGrouping( COUNT_BOLT_ID )

        when: 'cluster is started'
        def cluster = new LocalCluster()
        cluster.submitTopology( TOPOLOGY_NAME, new Config(), topologyBuilder.createTopology() )
        Thread.sleep( 10000 )
        cluster.killTopology( TOPOLOGY_NAME )
        cluster.shutdown()

        then: 'job is done'
    }
}
