package transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.sound.midi.SoundbankResource;
import java.util.HashMap;

/**
 * @author lihaoran
 */
public class TransformTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> fileSource = env.readTextFile("/Users/lihaoran/Documents/flink-projects/flink-test/src/main/resources/word.txt");
        SingleOutputStreamOperator<Integer> map = fileSource.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });
        // 传入接口或者抽象类

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordTupleStream = fileSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        });
//        wordTupleStream.print("flatmap");

        SingleOutputStreamOperator<String> filterStream = fileSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("hello");
            }
        });
//        filterStream.print("filter");

        fileSource.map(new RichMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("open");
            }

            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<String,Integer>(s,getRuntimeContext().getIndexOfThisSubtask());
            }

            @Override
            public void close() throws Exception {
                System.out.println("close");
            }
        }).print();

        env.execute();
    }
}
