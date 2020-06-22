package com.dtc.analytic.online.process.mapFunction;

import com.dtc.analytic.online.protobuf.MotorVehicleObjProto;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class vehicleFlatMapFunction implements FlatMapFunction<byte[],String> {
    @Override
    public void flatMap(byte[] bytes, Collector<String> collector) throws Exception {
        String deviceId = null;

        MotorVehicleObjProto.MotorVehicleList motorVehicleList = MotorVehicleObjProto.MotorVehicleList.parseFrom(bytes);
        List<MotorVehicleObjProto.MotorVehicle> MotorVehicles = motorVehicleList.getVehiclelistList();
        if (MotorVehicles != null) {
            for (MotorVehicleObjProto.MotorVehicle motorVehicle : MotorVehicles) {
                deviceId = motorVehicle.getDeviceId();
                collector.collect(deviceId);
            }
        } else {
            collector.collect(deviceId);
        }
    }
}
