package com.dtc.analytic.dataproducer.utils;

import com.dtc.analytic.online.protobuf.BaseStationObjProto;
import com.dtc.analytic.online.protobuf.MACCollectionRecordObjProto;
import com.dtc.analytic.online.protobuf.MotorVehicleObjProto;

import java.util.Random;

public class ProtoUtil {
    public static byte[] getBaseStationMessage() {
        BaseStationObjProto.BaseStationList baseStationList = BaseStationUtil.getBaseStationList();
        byte[] bytes = baseStationList.toByteArray();
        return bytes;
    }

    public static byte[] getWifiMessage() {
        MACCollectionRecordObjProto.MACCollectionRecordList wifiList = WifiUtil.getWifiList();
        byte[] bytes = wifiList.toByteArray();
        return bytes;
    }

    public static byte[] getVehicleMessage() {
        MotorVehicleObjProto.MotorVehicleList vehicleList = VehicleUtil.getVehicleList();
        byte[] bytes = vehicleList.toByteArray();
        return bytes;
    }

    public static String getCameraMessage() {
        String[] deviceIds={"test_s1,test_s2,test_s3","test_s8,test_s10","test_s2,test_s3,test_s9","test_s8,test_s10","test_s4,test_s6,test_s7"};
        Random random = new Random();
        int i = random.nextInt(5);
        return deviceIds[i];
    }
}
