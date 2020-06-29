package com.dtc.analytic.online.process.processFunction;

import com.dtc.analytic.online.common.modle.AlterStruct;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

import static com.dtc.analytic.online.common.utils.AlarmUtils.getOffLineAlarmResult;

public class getOffLineAlarmProcessFunction extends BroadcastProcessFunction<Tuple2<String,String>, Map<String, String>, AlterStruct> {
    MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
            "alarm_rules",
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO);

    @Override
    public void processElement(Tuple2<String, String> value, ReadOnlyContext ctx, Collector<AlterStruct> out) throws Exception {
        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
        String device_id = value.f0;
        String weiyi = device_id;

        AlterStruct alterStruct = new AlterStruct();

        // 离线告警
        String s = device_id + "." + "deviceOffline";
        if (broadcastState.contains(s)) {
            Tuple3<String, String, String> input = Tuple3.of(value.f0, s, value.f1);
            alterStruct = getOffLineAlarmResult(input, broadcastState);

            out.collect(alterStruct);
        }
    }

    @Override
    public void processBroadcastElement(Map<String, String> value, Context ctx, Collector<AlterStruct> collector) throws Exception {
        if (value == null || value.size() == 0) {
            return;
        }
        if (value != null) {
            BroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
            for (Map.Entry<String, String> entry : value.entrySet()) {
                broadcastState.put(entry.getKey(), entry.getValue());
            }
        }
    }
}
