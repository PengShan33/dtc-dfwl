package com.dtc.analytic.dataproducer.utils;

import com.dtc.analytic.online.protobuf.MotorVehicleObjProto;

import java.util.Random;

public class VehicleUtil {
    public static MotorVehicleObjProto.MotorVehicleList getVehicleList() {
        MotorVehicleObjProto.MotorVehicleList.Builder builder = MotorVehicleObjProto.MotorVehicleList.newBuilder();
        Random random = new Random();
//        int num = random.nextInt(3);
        for (int i = 0; i < 500; i++) {
            builder.addVehiclelist(getVehicle());
        }
        return builder.build();
    }

    private static MotorVehicleObjProto.MotorVehicle getVehicle() {
        MotorVehicleObjProto.MotorVehicle.Builder builder = MotorVehicleObjProto.MotorVehicle.newBuilder();
        // 30个
        builder.setRecordId(MyRandomUtil.getRandStr());
        builder.setBusinessId(MyRandomUtil.getRandStr());
        builder.setInfoKind(MyRandomUtil.getRandInt());
        builder.setSourceId(MyRandomUtil.getRandStr());
        builder.setStorageUrl1(MyRandomUtil.getRandStr());
        builder.setLeftTopX(MyRandomUtil.getRandInt());
        builder.setLeftTopY(MyRandomUtil.getRandInt());
        builder.setRightBtmX(MyRandomUtil.getRandInt());
        builder.setRightBtmY(MyRandomUtil.getRandInt());
        builder.setPlateLeftTopX(MyRandomUtil.getRandInt());
        builder.setPlateLeftTopY(MyRandomUtil.getRandInt());
        builder.setPlateRightBtmX(MyRandomUtil.getRandInt());
        builder.setPlateRightBtmY(MyRandomUtil.getRandInt());
        builder.setDeviceId(MyRandomUtil.getRandVehicleDeviceId());
        builder.setPlateNo(MyRandomUtil.getRandStr());
        builder.setPlateNoConfidence(MyRandomUtil.getRandTDouble());
        builder.setPlateClass(MyRandomUtil.getRandStr());
        builder.setPlateColor(MyRandomUtil.getRandStr());
        builder.setTrafficStatus(MyRandomUtil.getRandStr());
        builder.setVehicleType(MyRandomUtil.getRandStr());
        builder.setVehicleColor(MyRandomUtil.getRandStr());
        builder.setVehicleBrand(MyRandomUtil.getRandStr());
        builder.setVehicleBrandConfidence(MyRandomUtil.getRandTDouble());
        builder.setVehicleSubBrand(MyRandomUtil.getRandStr());
        builder.setVehicleSubBrandConfidence(MyRandomUtil.getRandTDouble());
        builder.setFaceNum(MyRandomUtil.getRandInt());
        builder.setSunVisor(MyRandomUtil.getRandInt());
        builder.setSeatBeltStatus(MyRandomUtil.getRandInt());
        builder.setAnnualInspectionNum(MyRandomUtil.getRandInt());
        builder.setPendantsNum(MyRandomUtil.getRandInt());

        // 30个
        builder.setOrnamentsNum(MyRandomUtil.getRandInt());
        builder.setVehicleYear(MyRandomUtil.getRandStr());
        builder.setTissueBoxNum(MyRandomUtil.getRandInt());
        builder.setCallStatus(MyRandomUtil.getRandInt());
        builder.setViolationType(MyRandomUtil.getRandStr());
        builder.setDataSource(MyRandomUtil.getRandStr());
        builder.setElapsedTime(MyRandomUtil.getRandInt());
        builder.setDataInKafkaTime(MyRandomUtil.getRandInt());
        builder.setEntryTime(MyRandomUtil.getRandInt());
        builder.setVehicleSpeed(MyRandomUtil.getRandInt());
        builder.setMarkerType(MyRandomUtil.getRandInt());
        builder.setVehicleHeadend(MyRandomUtil.getRandInt());
        builder.setShieldFace(MyRandomUtil.getRandInt());
        builder.setTaskType(MyRandomUtil.getRandStr());
        builder.setInfoSource(MyRandomUtil.getRandStr());
        builder.setAlgorithmVersion(MyRandomUtil.getRandStr());
        builder.setAlgorithmVendor(MyRandomUtil.getRandInt());
        builder.setEigenvector(MyRandomUtil.getRandStr());
        builder.setIsSecondaryStructure(MyRandomUtil.getRandInt());
        builder.setStructureTime(MyRandomUtil.getRandInt());
        builder.setCollectionId(MyRandomUtil.getRandStr());
        builder.setAdditionalInfo(MyRandomUtil.getRandStr());
        builder.setSource(MyRandomUtil.getRandInt());
        builder.setDrivewayNo(MyRandomUtil.getRandStr());
        builder.setVehicleLocation(MyRandomUtil.getRandStr());
        builder.setPlateLocation(MyRandomUtil.getRandStr());
        builder.setMainVisorPosition(MyRandomUtil.getRandStr());
        builder.setViceVisorPosition(MyRandomUtil.getRandStr());
        builder.setMainSeatBeltPosition(MyRandomUtil.getRandStr());
        builder.setViceSeatBeltPosition(MyRandomUtil.getRandStr());

        // 30个
        builder.setDrivingCall(MyRandomUtil.getRandStr());
        builder.setAnnualInspectionPosition(MyRandomUtil.getRandStr());
        builder.setPendantLocation(MyRandomUtil.getRandStr());
        builder.setTissueBoxLocation(MyRandomUtil.getRandStr());
        builder.setPlatePicUrl(MyRandomUtil.getRandStr());
        builder.setMoveDirection(MyRandomUtil.getRandInt());
        builder.setSpecialCar(MyRandomUtil.getRandInt());
        builder.setMoveSpeed(MyRandomUtil.getRandInt());
        // 特殊
//        builder.setSubImageList(0,SubImageUtil.getSubImage());
        builder.setStructureNum(MyRandomUtil.getRandInt());
        builder.setAccessTime(MyRandomUtil.getRandInt());
        builder.setHasPlate(MyRandomUtil.getRandStr());
        builder.setStructureSource(MyRandomUtil.getRandInt());
        builder.setTollgateId(MyRandomUtil.getRandStr());
        builder.setLaneNO(MyRandomUtil.getRandInt());
        builder.setWholeConfidence(MyRandomUtil.getRandInt());
        builder.setDriverPhoneConfidence(MyRandomUtil.getRandInt());
        builder.setDeputyDriverPhoneConfidence(MyRandomUtil.getRandInt());
        builder.setDriverSeatBeltConfidence(MyRandomUtil.getRandInt());
        builder.setDeputyDriverSeatBeltConfidence(MyRandomUtil.getRandInt());
        builder.setRoofItems(MyRandomUtil.getRandInt());
        builder.setPlateOcclusion(MyRandomUtil.getRandInt());
        builder.setFacialOcclusion(MyRandomUtil.getRandInt());
        builder.setExt(MyRandomUtil.getRandStr());
        builder.setRelatedType(MyRandomUtil.getRandStr());
        // 特殊
//        builder.setRelatedList(0,RelatedInfoUtil.getRelatedInfo());
        builder.setLatitude(MyRandomUtil.getRandInt());
        builder.setLongitude(MyRandomUtil.getRandInt());
        builder.setDataMarks(MyRandomUtil.getRandInt());
        builder.setTraceInfo(MyRandomUtil.getRandStr());

        return builder.build();
    }
}
