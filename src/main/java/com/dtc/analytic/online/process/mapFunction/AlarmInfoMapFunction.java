package com.dtc.analytic.online.process.mapFunction;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple10;

import java.util.HashMap;
import java.util.Map;

public class AlarmInfoMapFunction implements MapFunction<Map<String, Tuple10<String, String, String, Double, Double, Double, Double, String, String, String>>, Map<String, String>> {
    @Override
    public Map<String, String> map(Map<String, Tuple10<String, String, String, Double, Double, Double, Double, String, String, String>> event) throws Exception {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, Tuple10<String, String, String, Double, Double, Double, Double, String, String, String>> entries : event.entrySet()) {
            Tuple10<String, String, String, Double, Double, Double, Double, String, String, String> value = entries.getValue();
            String key = entries.getKey();
            String asset_id = value.f0;
            String device_id = value.f1;
            String code = value.f2;
            Double level_1 = value.f3;
            Double level_2 = value.f4;
            Double level_3 = value.f5;
            Double level_4 = value.f6;
            String asset_code = value.f7;
            String asset_name = value.f8;
            String triger_name = value.f9;
            String str = asset_id + ":" + device_id + ":" + code + ":" + asset_code + ":" + asset_name + ":" + level_1 + "|" + level_2 + "|" + level_3 + "|" + level_4 + ":" + triger_name;
            map.put(key, str);
        }
        return map;
    }
}
