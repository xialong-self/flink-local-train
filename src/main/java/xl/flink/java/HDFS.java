package xl.flink.java;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.util.Collector;
import org.apache.lucene.document.StringField;

/**
 * @author 夏龙
 * @date 2020-09-21
 */



public class HDFS {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从文本文件中数据并切分打散
        DataStream<String> data = env.readTextFile("C:\\Users\\xialong\\Desktop\\2020-08-25Scala开始\\query-hive-1392.csv")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        String[] arr = s.split(",");
                        for(String string:arr){
                            collector.collect(string);
                        }
                    }
                });
//        // 创建新版本的hdfs sink
//        final StreamingFileSink<String> sink = StreamingFileSink
//                // 设置输出路径以及字符类型,输出的文件格式为/path/{date-time}/part-{parallel-task}-{count}
//                .forRowFormat(new Path("hdfs://hadoopNode1:9000/home/flink"), new SimpleStringEncoder<String>("UTF-8"))
//                .build();
//        // 添加沉槽
//        data.addSink(sink);
        data.print();
        // 执行任务
        env.execute("FileSystem connector");
    }
}

