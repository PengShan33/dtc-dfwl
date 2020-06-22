package com.dtc.analytic.online.process.mapFunction;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class cameraFlatMapFunction implements FlatMapFunction<String, String> {
    @Override
    public void flatMap(String line, Collector<String> collector) throws Exception {
        String[] deviceIds = line.split(",");
        if (deviceIds != null) {
            for (String deviceId : deviceIds) {
                collector.collect(deviceId);
            }
        }
    }
}
