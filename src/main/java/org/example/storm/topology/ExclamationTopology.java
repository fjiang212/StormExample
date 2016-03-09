package org.example.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopology {

public static void main(String[] args) throws Exception {
	TopologyBuilder builder = new TopologyBuilder();

	builder.setSpout("word", new TestWordSpout(), 10);
	builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("word");
	builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");

	Config conf = new Config();
	conf.setDebug(true);

	//run topology in the cluster mode
	if (args != null && args.length > 0) {
		conf.setNumWorkers(3);

		StormSubmitter.submitTopologyWithProgressBar(args[0], conf,	builder.createTopology());
	} 
	//run topology in the local mode
	else {
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology("test");
		cluster.shutdown();
	}
}

public static class ExclamationBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	OutputCollector mCollector;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		mCollector = collector;
	}

	public void execute(Tuple tuple) {
		mCollector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
		mCollector.ack(tuple);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}
}
