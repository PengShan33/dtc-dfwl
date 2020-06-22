package com.dtc.analytic.dataproducer.utils;

import com.dtc.analytic.online.protobuf.RelatedInfoObjProto;

public class RelatedInfoUtil {
    public static RelatedInfoObjProto.RelatedInfo getRelatedInfo() {
        RelatedInfoObjProto.RelatedInfo.Builder builder = RelatedInfoObjProto.RelatedInfo.newBuilder();

        builder.setRelatedType(MyRandomUtil.getRandStr());
        builder.setRelatedId(MyRandomUtil.getRandStr());
        builder.setRelatedNPId(MyRandomUtil.getRandStr());

        return builder.build();
    }
}
