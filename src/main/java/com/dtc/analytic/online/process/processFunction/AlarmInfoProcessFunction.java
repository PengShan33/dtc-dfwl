package com.dtc.analytic.online.process.processFunction;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class AlarmInfoProcessFunction extends ProcessWindowFunction<Tuple9<String, String, String, String, Double, String, String, String, String>, Map<String, Tuple10<String, String, String, Double, Double, Double, Double, String, String,String>>, Tuple, TimeWindow> {
    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple9<String, String, String, String, Double, String, String, String, String>> iterable, Collector<Map<String, Tuple10<String, String, String, Double, Double, Double, Double, String, String,String>>> collector) throws Exception {
        //asset_id, device_id, strategy_kind, triger_name, number, code, alarm_level, asset_code, name
        Tuple10<String, String, String, Double, Double, Double, Double, String, String, String> tuple10 = new Tuple10<>();
        Map<String, Tuple10<String, String, String, Double, Double, Double, Double, String, String, String>> map = new HashMap<>();
        for (Tuple9<String, String, String, String, Double, String, String, String, String> sourceEvent : iterable) {
            String asset_id = sourceEvent.f0;
            String device_id = sourceEvent.f1;
            String triger_name = sourceEvent.f3;
            Double num = sourceEvent.f4;
            String code = sourceEvent.f5;
            String level = sourceEvent.f6;
            tuple10.f0 = asset_id;
            tuple10.f1 = device_id;
            tuple10.f2 = code;
            String key = device_id + "." + code.replace("_", ".");
            if ("1".equals(level)) {
                tuple10.f3 = num;
            } else if ("2".equals(level)) {
                tuple10.f4 = num;
            } else if ("3".equals(level)) {
                tuple10.f5 = num;
            } else if ("4".equals(level)) {
                tuple10.f6 = num;
            }
            tuple10.f7 = sourceEvent.f7;
            tuple10.f8 = sourceEvent.f8;
            tuple10.f9 = triger_name;
            map.put(key, tuple10);
        }
        collector.collect(map);
    }

}
