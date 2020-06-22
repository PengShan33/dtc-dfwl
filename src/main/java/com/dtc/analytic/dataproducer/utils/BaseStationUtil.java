package com.dtc.analytic.dataproducer.utils;

import com.dtc.analytic.online.protobuf.BaseStationObjProto;

import java.util.Random;

public class BaseStationUtil {
    public static BaseStationObjProto.BaseStationList getBaseStationList() {
        BaseStationObjProto.BaseStationList.Builder builder = BaseStationObjProto.BaseStationList.newBuilder();

        Random random = new Random();
        // int num = random.nextInt(3);
        for (int i = 0; i < 500; i++) {
            builder.addBasestation(getBaseStation());
        }

        return builder.build();
    }

    private static BaseStationObjProto.BaseStation getBaseStation() {
        BaseStationObjProto.BaseStation.Builder builder = BaseStationObjProto.BaseStation.newBuilder();

        builder.setBegintime(MyRandomUtil.getRandTime());
        builder.setEvent(MyRandomUtil.getRandStr());
        builder.setUsernum(MyRandomUtil.getRandStr());
        builder.setHomearea(MyRandomUtil.getRandStr());
        builder.setRelatenum(MyRandomUtil.getRandStr());
        builder.setRelatehomeac(MyRandomUtil.getRandStr());
        builder.setImsi(MyRandomUtil.getRandStr());
        builder.setImei(MyRandomUtil.getRandStr());
        builder.setCurarea(MyRandomUtil.getRandStr());
        builder.setNeid(MyRandomUtil.getRandStr());
        builder.setLai(MyRandomUtil.getRandStr());
        builder.setCi(MyRandomUtil.getRandStr());
        builder.setLongitude(MyRandomUtil.getRandTDouble());
        builder.setLatitude(MyRandomUtil.getRandTDouble());
        builder.setOldlai(MyRandomUtil.getRandStr());
        builder.setOldci(MyRandomUtil.getRandStr());
        builder.setOldlongitude(MyRandomUtil.getRandTDouble());
        builder.setOldlatitude(MyRandomUtil.getRandTDouble());
        builder.setRecordId(MyRandomUtil.getRandStr());
        builder.setBusinessId(MyRandomUtil.getRandStr());
        builder.setAccessTime(MyRandomUtil.getRandTime());
        builder.setDeviceId(MyRandomUtil.getRandBaseStationDeviceId());
        builder.setImsiUserNum(MyRandomUtil.getRandStr());
        builder.setHomeareaAddress(MyRandomUtil.getRandStr());
        builder.setRSSI(MyRandomUtil.getRandStr());

        BaseStationObjProto.BaseStation baseStation = builder.build();

        return baseStation;
    }
}
