package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author lihaoran
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // TODO 批处理
//        ExecutionEnvironment envSet = ExecutionEnvironment.getExecutionEnvironment();
//
//        String inputPath = "/Users/lihaoran/Documents/flink-projects/flink-learn/src/main/resources/hello.txt";
//        DataSource<String> dataSource = envSet.readTextFile(inputPath);
//        DataSet<Tuple2<String, Integer>> result = dataSource.flatMap(new MyFlatMapper())
//                //按第一位分组
//                .groupBy(0)
//                //按第二位求和
//                .sum(1);
//        result.print();
        // TODO 流处理
        StreamExecutionEnvironment envStream = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = envStream.readTextFile("/Users/lihaoran/Documents/flink-projects/flink-learn/src/main/resources/hello.txt");
        dataStream.flatMap(new MyFlatMapper()).keyBy(0)
                .sum(1)
                .print();
        envStream.execute();
    }

    private static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
