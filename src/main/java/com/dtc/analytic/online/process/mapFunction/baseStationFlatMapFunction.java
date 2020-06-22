package com.dtc.analytic.online.process.mapFunction;

import com.dtc.analytic.online.protobuf.BaseStationObjProto;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class baseStationFlatMapFunction implements FlatMapFunction<byte[], String> {
    @Override
    public void flatMap(byte[] bytes, Collector<String> collector) throws Exception {
        String deviceId = null;

        BaseStationObjProto.BaseStationList baseStationList = BaseStationObjProto.BaseStationList.parseFrom(bytes);
        List<BaseStationObjProto.BaseStation> baseStations = baseStationList.getBasestationList();
        if (baseStations!=null) {
            for (BaseStationObjProto.BaseStation baseStation : baseStations) {
                deviceId = baseStation.getDeviceId();
                collector.collect(deviceId);
            }
        } else {
            collector.collect(deviceId);
        }
    }
}
