package com.dtc.analytic.online.process.processFunction;

import com.dtc.analytic.online.common.modle.AlterStruct;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

import static com.dtc.analytic.online.common.utils.AlarmUtils.getAlarmResult;

public class getAlarmProcessFunction extends BroadcastProcessFunction<Tuple5<String, Double, Double, Double, Double>, Map<String, String>, AlterStruct> {

    MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
            "alarm_rules",
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    // value: device_id,在线时长,离线时长,持续离线时长,在线率
    @Override
    public void processElement(Tuple5<String, Double, Double, Double, Double> value, ReadOnlyContext ctx, Collector<AlterStruct> out) throws Exception {
//        Thread.sleep(1000 * 10);
        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);

//        String s = broadcastState.get("s8.142");
//        System.out.println(s);

        String device_id = value.f0;

        AlterStruct alterStruct;

        // 离线保持时长告警
        String s1 = device_id + "." + "140";
        // 累计离线时长告警
        String s2 = device_id + "." + "141";
        // 离线率告警
        String s3 = device_id + "." + "142";
        if (broadcastState.contains(s1)) {
            Tuple3<String, String, Double> input = Tuple3.of(value.f0, s1, value.f3);
            alterStruct = getAlarmResult(input, broadcastState);

            out.collect(alterStruct);
        } else if (broadcastState.contains(s2)) {
            Tuple3<String, String, Double> input = Tuple3.of(value.f0, s2, value.f2);
            alterStruct = getAlarmResult(input, broadcastState);

            out.collect(alterStruct);
        } else if (broadcastState.contains(s3)) {
            if (value.f4 >= 0) {
                Double rate = (1 - value.f4) * 100;
                Tuple3<String, String, Double> input = Tuple3.of(value.f0, s3, rate);
                alterStruct = getAlarmResult(input, broadcastState);

                out.collect(alterStruct);
            } else {return;}

        } else { return; }
        broadcastState.clear();

//        if (alterStruct.getUnique_id() != null) {
//            out.collect(alterStruct);
//            broadcastState.clear();
//        }
    }
    Runnable runnable=null;
    @Override
    public void processBroadcastElement(Map<String, String> value, Context ctx, Collector<AlterStruct> collector) throws Exception {
        BroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
        if (value == null || value.size() == 0) {
//            broadcastState.clear();
            return;
        }
        if (value != null) {
            for (Map.Entry<String, String> entry : value.entrySet()) {
                broadcastState.put(entry.getKey(), entry.getValue());
            }
        }
    }

}
