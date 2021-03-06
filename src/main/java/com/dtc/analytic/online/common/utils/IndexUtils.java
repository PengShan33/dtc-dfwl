package com.dtc.analytic.online.common.utils;

import com.dtc.analytic.online.process.mapFunction.IndexCalculateFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

public class IndexUtils {
    public static DataStream<Tuple5<String, Double, Double,  Double, Double>> indexCalculate(DataStreamSource<String> inputStream) {
        DataStream<Tuple5<String, Double, Double, Double, Double>> resultStream = inputStream.flatMap(new IndexCalculateFlatMapFunction());
        return resultStream;
    }
}
