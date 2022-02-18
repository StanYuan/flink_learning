package com.zhisheng.window;

import com.zhisheng.common.model.WordEvent;
import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.function.CustomSource;
import com.zhisheng.function.CustomTrigger;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.time.Duration;

/**
 * Desc:
 *  操作：在终端执行 nc -l 9000 ，然后输入 long text 类型的数据
 * Created by zhisheng on 2019-08-06
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CustomTriggerMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果不指定时间的话，默认是 ProcessingTime，但是如果指定为事件事件的话，需要事件中带有时间或者添加时间水印
        DataStream<WordEvent> data = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WordEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp()));
        data.keyBy(WordEvent::getWord)
//                .timeWindow(Time.seconds(10))
                //.trigger(CustomTrigger.creat())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("count")
                .print();
        env.execute("zhisheng  custom Trigger Window demo");
    }
}
