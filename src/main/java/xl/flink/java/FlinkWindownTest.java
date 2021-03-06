package xl.flink.java;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * @author 夏龙
 * @date 2020-09-17
 */

public class FlinkWindownTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //如果是划分窗口，如果没有调用keyBy分组（Non-Keyed Stream），调用windowAll
        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        //传入窗口分配器（划分器），传入具体划分窗口规则
        //CountWindw：按照条数划分窗口
        AllWindowedStream<Integer, GlobalWindow> window = nums.countWindowAll(5);

        SingleOutputStreamOperator<Integer> result = window.sum(0);

        result.print();

        env.execute();
    }
}
