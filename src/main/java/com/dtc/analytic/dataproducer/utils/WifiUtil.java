package com.dtc.analytic.dataproducer.utils;

import com.dtc.analytic.online.protobuf.MACCollectionRecordObjProto;

import java.util.Random;

public class WifiUtil {
    public static MACCollectionRecordObjProto.MACCollectionRecordList getWifiList() {
        MACCollectionRecordObjProto.MACCollectionRecordList.Builder builder = MACCollectionRecordObjProto.MACCollectionRecordList.newBuilder();

        Random random = new Random();
//        int num = random.nextInt(3);
        for (int i = 0; i < 500; i++) {
            builder.addMaclist(getMACCollectionRecord());
        }

        return builder.build();
    }

    private static MACCollectionRecordObjProto.MACCollectionRecord getMACCollectionRecord() {
        MACCollectionRecordObjProto.MACCollectionRecord.Builder builder = MACCollectionRecordObjProto.MACCollectionRecord.newBuilder();

        builder.setRecordId(MyRandomUtil.getRandStr());
        builder.setBusinessId(MyRandomUtil.getRandStr());
        builder.setDeviceId(MyRandomUtil.getRandMACDeviceId());
        builder.setDeviceName(MyRandomUtil.getRandStr());
        builder.setPlaceCode(MyRandomUtil.getRandStr());
        builder.setPlaceName(MyRandomUtil.getRandStr());
        builder.setAbsTime(MyRandomUtil.getRandTime());
        builder.setUserMac(MyRandomUtil.getRandStr());
        builder.setMac(MyRandomUtil.getRandStr());
        builder.setRssi(MyRandomUtil.getRandStr());
        builder.setBrand(MyRandomUtil.getRandStr());
        builder.setSsidList(MyRandomUtil.getRandStr());
        builder.setXCoordinate(MyRandomUtil.getRandTDouble());
        builder.setYCoordinate(MyRandomUtil.getRandTDouble());
        builder.setDistance(MyRandomUtil.getRandTDouble());
        builder.setPhoneNumber(MyRandomUtil.getRandStr());
        builder.setImsi(MyRandomUtil.getRandStr());
        builder.setImei(MyRandomUtil.getRandStr());
        builder.setAccountNumber(MyRandomUtil.getRandStr());
        builder.setExtraInfo(MyRandomUtil.getRandStr());
        builder.setApConnected(MyRandomUtil.getRandInt());
        builder.setApMac(MyRandomUtil.getRandStr());
        builder.setApSsid(MyRandomUtil.getRandStr());
        builder.setApFrequency(MyRandomUtil.getRandInt());
        builder.setApChannel(MyRandomUtil.getRandInt());
        builder.setApAuxChannel(MyRandomUtil.getRandInt());
        builder.setApEncryptType(MyRandomUtil.getRandStr());
        builder.setPushTime(MyRandomUtil.getRandTime());
        builder.setAccessTime(MyRandomUtil.getRandTime());

        return builder.build();
    }
}
