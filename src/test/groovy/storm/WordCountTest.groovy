package storm

import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields
import spock.lang.Specification

class WordCountTest extends Specification {

    final String SENTENCE_SPOUT_ID = 'sentence-spout'
    final String SPLIT_BOLT_ID = 'splitting-bolt'
    final String COUNT_BOLT_ID = 'counting-bolt'
    final String REPORT_BOLT_ID = 'reporting-bolt'
    final String TOPOLOGY_NAME = 'word-count-topology'

    def 'exercise topology'() {

        given: 'a valid topology'
        def topologyBuilder = new TopologyBuilder()
        topologyBuilder.setSpout( SENTENCE_SPOUT_ID, new SentenceSpout() )
        topologyBuilder.setBolt( SPLIT_BOLT_ID, new SplitSentenceBolt() ).setNumTasks( 2 )
                       .shuffleGrouping( SENTENCE_SPOUT_ID )
        topologyBuilder.setBolt( COUNT_BOLT_ID, new WordCountBolt(), 2 )
                        .fieldsGrouping(SPLIT_BOLT_ID, new Fields( FieldNames.word.name() ) )
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
