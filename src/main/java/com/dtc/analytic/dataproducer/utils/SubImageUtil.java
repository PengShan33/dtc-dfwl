package com.dtc.analytic.dataproducer.utils;

import com.dtc.analytic.online.protobuf.SubImageInfoObjProto;

import java.util.ArrayList;

public class SubImageUtil {
    public static SubImageInfoObjProto.SubImageInfo getSubImage() {
        ArrayList<SubImageInfoObjProto.SubImageInfo> subImageInfos = new ArrayList<>();

        SubImageInfoObjProto.SubImageInfo.Builder builder = SubImageInfoObjProto.SubImageInfo.newBuilder();

        builder.setDeviceId(MyRandomUtil.getRandVehicleDeviceId());
        builder.setImageId(MyRandomUtil.getRandStr());
        builder.setImageSource(MyRandomUtil.getRandStr());
        builder.setTitle(MyRandomUtil.getRandStr());
        builder.setImageData(MyRandomUtil.getRandByteStr());
        builder.setFileFormat(MyRandomUtil.getRandStr());
        builder.setShotPlaceFullAdress(MyRandomUtil.getRandStr());
        builder.setShotTime(MyRandomUtil.getRandInt());
        builder.setEventSort(MyRandomUtil.getRandInt());
        builder.setImageType(MyRandomUtil.getRandInt());
        builder.setInfoKind(MyRandomUtil.getRandInt());
        builder.setStoragePath(MyRandomUtil.getRandStr());
        builder.setContentDescription(MyRandomUtil.getRandStr());

        return builder.build();
    }
}
