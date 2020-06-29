package com.dtc.analytic.online.main;

import com.dtc.analytic.online.common.modle.TimesConstants;
import com.dtc.analytic.online.process.joinFunction.LeftJoinResult;
import com.dtc.analytic.online.process.joinFunction.LeftSelectKey;
import com.dtc.analytic.online.process.joinFunction.RightSelectKey;
import com.dtc.analytic.online.process.mapFunction.baseStationFlatMapFunction;
import com.dtc.analytic.online.process.mapFunction.cameraFlatMapFunction;
import com.dtc.analytic.online.process.mapFunction.vehicleFlatMapFunction;
import com.dtc.analytic.online.process.mapFunction.wifiFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class MainUtils {

    public static DataStream<String> byteDataProcess(DataStreamSource<byte[]> streamSource, String topic) {
        DataStream<String> dataStream = null;
        if (topic.contains("vehicle")) {
            dataStream = streamSource.flatMap(new vehicleFlatMapFunction());
        }
        if (topic.contains("wifi")) {
            dataStream = streamSource.flatMap(new wifiFlatMapFunction());
        }
        if (topic.contains("basestation")) {
            dataStream = streamSource.flatMap(new baseStationFlatMapFunction());
        }
        return dataStream;
    }

    public static DataStream<String> stringDataProcess(DataStreamSource<String> streamSource, String topic) {
        DataStream<String> dataStream = null;
        dataStream = streamSource.flatMap(new cameraFlatMapFunction());
        return dataStream;
    }

    public static DataStream<Tuple2<String, String>> leftJoin(DataStreamSource<String> leftData, DataStream<String> rightData) {
        return leftData.coGroup(rightData)
                .where(new LeftSelectKey())
                .equalTo(new RightSelectKey())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(60 * 5)))
                .apply(new LeftJoinResult());
    }


    public static TimesConstants getSize(ParameterTool parameterTool) {
        int anInt_one = parameterTool.getInt("dtc.alarm.times.one", 1);
        int anInt1_one = parameterTool.getInt("dtc.alarm.time.long.one", 60000);
        int anInt_two = parameterTool.getInt("dtc.alarm.times.two", 1);
        int anInt1_two = parameterTool.getInt("dtc.alarm.time.long.two", 60000);
        TimesConstants build = TimesConstants.builder().one(anInt_one).two(anInt1_one).three(anInt_two).four(anInt1_two).build();
        return build;
    }
}
