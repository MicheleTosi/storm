package org.apache.storm.topology;

import org.apache.storm.hooks.IWorkerHook;
import org.apache.storm.lambda.SerializableSupplier;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.state.State;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TimestampExtractor;
import org.apache.storm.windowing.TupleWindow;

import java.io.ObjectOutputStream;
import java.util.Map;

public class Utils {

    public IRichBolt makeRichBolt() {
        return new IRichBolt() {
            @Override
            public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            }

            @Override
            public void execute(Tuple input) {
            }

            @Override
            public void cleanup() {
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }

            @Override
            public Map<String, Object> getComponentConfiguration() {
                return null;
            }

            private void writeObject(ObjectOutputStream stream) {
            }
        };
    }

    public IRichBolt makeUnserializableRichBolt() {
        return new IRichBolt() {
            @Override
            public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            }

            @Override
            public void execute(Tuple input) {
            }

            @Override
            public void cleanup() {
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }

            @Override
            public Map<String, Object> getComponentConfiguration() {
                return null;
            }
        };
    }

    public IBasicBolt makeBasicBolt() {
        return new IBasicBolt() {
            @Override
            public void prepare(Map<String, Object> topoConf, TopologyContext context) {

            }

            @Override
            public void execute(Tuple input, BasicOutputCollector collector) {

            }

            @Override
            public void cleanup() {

            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {

            }

            @Override
            public Map<String, Object> getComponentConfiguration() {
                return null;
            }

            private void writeObject(ObjectOutputStream stream) {
            }
        };
    }

    public IBasicBolt makeUnserializableBasicBolt() {
        return new IBasicBolt() {
            @Override
            public void prepare(Map<String, Object> topoConf, TopologyContext context) {

            }

            @Override
            public void execute(Tuple input, BasicOutputCollector collector) {

            }

            @Override
            public void cleanup() {

            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {

            }

            @Override
            public Map<String, Object> getComponentConfiguration() {
                return null;
            }
        };
    }

    public IWindowedBolt makeWindowedBolt() {
        return new IWindowedBolt() {


            @Override
            public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

            }

            @Override
            public void execute(TupleWindow inputWindow) {

            }

            @Override
            public void cleanup() {

            }

            @Override
            public TimestampExtractor getTimestampExtractor() {
                return null;
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {

            }

            @Override
            public Map<String, Object> getComponentConfiguration() {
                return null;
            }

            private void writeObject(ObjectOutputStream stream) {
            }
        };
    }

    public IWindowedBolt makeUnserializableWindowedBolt() {
        return new IWindowedBolt() {

            @Override
            public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

            }

            @Override
            public void execute(TupleWindow inputWindow) {

            }

            @Override
            public void cleanup() {

            }

            @Override
            public TimestampExtractor getTimestampExtractor() {
                return null;
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {

            }

            @Override
            public Map<String, Object> getComponentConfiguration() {
                return null;
            }
        };
    }

    public IStatefulBolt<State> makeStatefulBolt() {
        return new IStatefulBolt<State>() {
            @Override
            public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

            }

            @Override
            public void execute(Tuple input) {

            }

            @Override
            public void cleanup() {

            }

            @Override
            public void initState(State state) {

            }

            @Override
            public void preCommit(long txid) {

            }

            @Override
            public void prePrepare(long txid) {

            }

            @Override
            public void preRollback() {

            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {

            }

            @Override
            public Map<String, Object> getComponentConfiguration() {
                return null;
            }

            private void writeObject(ObjectOutputStream stream) {
            }

        };
    }

    public IStatefulBolt<State> makeUnserializableStatefulBolt() {
        return new IStatefulBolt<State>() {
            @Override
            public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

            }

            @Override
            public void execute(Tuple input) {

            }

            @Override
            public void cleanup() {

            }

            @Override
            public void initState(State state) {

            }

            @Override
            public void preCommit(long txid) {

            }

            @Override
            public void prePrepare(long txid) {

            }

            @Override
            public void preRollback() {

            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {

            }

            @Override
            public Map<String, Object> getComponentConfiguration() {
                return null;
            }
        };
    }

    public IStatefulWindowedBolt<State> makeStatefulWindowedBolt() {
        return new IStatefulWindowedBolt<State>() {

            @Override
            public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

            }

            @Override
            public void execute(TupleWindow inputWindow) {

            }

            @Override
            public void cleanup() {

            }

            @Override
            public TimestampExtractor getTimestampExtractor() {
                return null;
            }

            @Override
            public void initState(State state) {

            }

            @Override
            public void preCommit(long txid) {

            }

            @Override
            public void prePrepare(long txid) {

            }

            @Override
            public void preRollback() {

            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {

            }

            @Override
            public Map<String, Object> getComponentConfiguration() {
                return null;
            }

            private void writeObject(ObjectOutputStream stream) {
            }

        };
    }

    public IStatefulWindowedBolt<State> makeStatefulPersistentWindowedBolt() {
        return new IStatefulWindowedBolt<State>() {


            @Override
            public boolean isPersistent() {
                return true;
            }

            @Override
            public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

            }

            @Override
            public void execute(TupleWindow inputWindow) {

            }

            @Override
            public void cleanup() {

            }

            @Override
            public TimestampExtractor getTimestampExtractor() {
                return null;
            }

            @Override
            public void initState(State state) {

            }

            @Override
            public void preCommit(long txid) {

            }

            @Override
            public void prePrepare(long txid) {

            }

            @Override
            public void preRollback() {

            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {

            }

            @Override
            public Map<String, Object> getComponentConfiguration() {
                return null;
            }

            private void writeObject(ObjectOutputStream stream) {
            }

        };
    }

    public IStatefulWindowedBolt<State> makeUnserializableStatefulWindowedBolt() {
        return new IStatefulWindowedBolt<State>() {

            @Override
            public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

            }

            @Override
            public void execute(TupleWindow inputWindow) {

            }

            @Override
            public void cleanup() {

            }

            @Override
            public TimestampExtractor getTimestampExtractor() {
                return null;
            }

            @Override
            public void initState(State state) {

            }

            @Override
            public void preCommit(long txid) {

            }

            @Override
            public void prePrepare(long txid) {

            }

            @Override
            public void preRollback() {

            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {

            }

            @Override
            public Map<String, Object> getComponentConfiguration() {
                return null;
            }
        };
    }

    public IRichSpout makeRichSpout() {
        return new IRichSpout() {
            @Override
            public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {

            }

            @Override
            public void close() {

            }

            @Override
            public void activate() {

            }

            @Override
            public void deactivate() {

            }

            @Override
            public void nextTuple() {

            }

            @Override
            public void ack(Object msgId) {

            }

            @Override
            public void fail(Object msgId) {

            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {

            }

            @Override
            public Map<String, Object> getComponentConfiguration() {
                return null;
            }

            private void writeObject(ObjectOutputStream stream) {
            }
        };
    }

    public IRichSpout makeUnserializableRichSpout() {
        return new IRichSpout() {
            @Override
            public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {

            }

            @Override
            public void close() {

            }

            @Override
            public void activate() {

            }

            @Override
            public void deactivate() {

            }

            @Override
            public void nextTuple() {

            }

            @Override
            public void ack(Object msgId) {

            }

            @Override
            public void fail(Object msgId) {

            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {

            }

            @Override
            public Map<String, Object> getComponentConfiguration() {
                return null;
            }
        };
    }

    public SerializableSupplier<Object> makeSerializableSupplier() {
        return new SerializableSupplier<Object>() {

            @Override
            public Object get() {
                return new Object();
            }

            private void writeObject(ObjectOutputStream stream) {
            }
        };
    }

    public SerializableSupplier<Object> makeUnserializableSupplier() {
        return new SerializableSupplier<Object>() {

            @Override
            public Object get() {
                return new Object();
            }
        };
    }

    public IWorkerHook makeWorkerHook(){
        return new IWorkerHook() {
            @Override
            public void start(Map<String, Object> topoConf, WorkerTopologyContext context) {

            }

            @Override
            public void shutdown() {

            }

            private void writeObject(ObjectOutputStream stream){}
        };
    }

    public IWorkerHook makeUnserializableWorkerHook(){
        return new IWorkerHook() {
            @Override
            public void start(Map<String, Object> topoConf, WorkerTopologyContext context) {

            }

            @Override
            public void shutdown() {

            }
        };
    }
}
