package ro.tucn.storm.bolt.windowed;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import ro.tucn.exceptions.DurationException;
import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.storm.bolt.constants.BoltConstants;
import ro.tucn.storm.datastructure.BTree;
import ro.tucn.util.TimeDuration;
import scala.Tuple2;

/**
 * Created by Liviu on 4/18/2017.
 */
public class WindowPairReduceBolt<K, V> extends WindowedBolt {

    private static final Logger logger = Logger.getLogger(WindowPairReduceByKeyBolt.class);

    // for each slide, there is a corresponding reduced Tuple2
    private BTree<Tuple2<K, V>> reduceDataContainer;
    private ReduceFunction<Tuple2<K, V>> fun;

    public WindowPairReduceBolt(ReduceFunction<Tuple2<K, V>> function,
                                TimeDuration windowDuration,
                                TimeDuration slideDuration) throws DurationException {
        super(windowDuration, slideDuration);
        this.fun = function;
        reduceDataContainer = new BTree<>(WINDOW_SIZE);
    }

    /**
     * called after receiving a normal tuple
     *
     * @param tuple
     */
    @Override
    public void processTuple(Tuple tuple) {
        try {
            Tuple2<K, V> reducedTuple = reduceDataContainer.get(slideInWindow);
            K key = (K) tuple.getValue(0);
            V value = (V) tuple.getValue(1);
            Tuple2<K, V> tuple2 = new Tuple2<>(key, value);
            if (null == reducedTuple)
                reducedTuple = tuple2;
            else {
                reducedTuple = fun.reduce(reducedTuple, tuple2);
            }
            reduceDataContainer.set(slideInWindow, reducedTuple);
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    /**
     * called after receiving a tick tuple
     * reduce all data(slides) in current window
     *
     * @param collector
     */
    @Override
    public void processSlide(BasicOutputCollector collector) {
        try {
            // update slideInWindow node and its parents until root
            // single slide window
            if (!reduceDataContainer.isRoot(slideInWindow)) {
                int updatedNode = slideInWindow;
                // if latest updated node is not root, update its parent node
                while (!reduceDataContainer.isRoot(updatedNode)) {
                    int parent = reduceDataContainer.findParent(updatedNode);
                    BTree.Children children = reduceDataContainer.findChildren(parent);
                    if (null == reduceDataContainer.get(children.getChild1())
                            || null == reduceDataContainer.get(children.getChild2())) {
                        reduceDataContainer.set(parent, reduceDataContainer.get(updatedNode));
                    } else {
                        reduceDataContainer.set(parent,
                                fun.reduce(reduceDataContainer.get(children.getChild1()), reduceDataContainer.get(children.getChild2())));
                    }
                    updatedNode = parent;
                }
            }
            Tuple2<K, V> root = reduceDataContainer.getRoot();
            collector.emit(new Values(slideIndexInBuffer, root._1(), root._2()));
            // clear data
            reduceDataContainer.set((slideInWindow + 1) % WINDOW_SIZE, null);
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(new Fields(BoltConstants.OutputSlideIdField, BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
