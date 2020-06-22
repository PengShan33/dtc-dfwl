package com.dtc.analytic.online.process.mapFunction;

import com.dtc.analytic.online.protobuf.MACCollectionRecordObjProto;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class wifiFlatMapFunction implements FlatMapFunction<byte[], String> {
    @Override
    public void flatMap(byte[] bytes, Collector<String> collector) throws Exception {
        String deviceId = null;
        MACCollectionRecordObjProto.MACCollectionRecordList macCollectionRecordList = MACCollectionRecordObjProto.MACCollectionRecordList.parseFrom(bytes);
        List<MACCollectionRecordObjProto.MACCollectionRecord> macCollectionRecords = macCollectionRecordList.getMaclistList();

        // List中的deviceId都相同
//        MACCollectionRecordObjProto.MACCollectionRecord macCollectionRecordSingle = macCollectionRecordList.getMaclist(0);
//        deviceId = macCollectionRecordSingle.getDeviceId();
//        collector.collect(deviceId);

        // List中的deviceId不同
        if (macCollectionRecords != null) {
            for (MACCollectionRecordObjProto.MACCollectionRecord macCollectionRecord : macCollectionRecords) {
                deviceId = macCollectionRecord.getDeviceId();
                collector.collect(deviceId);
            }
        } else {
            collector.collect(deviceId);
        }
    }
}
