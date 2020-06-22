package com.dtc.analytic.dataproducer.utils;

import com.google.protobuf.ByteString;
import org.apache.commons.lang.RandomStringUtils;

import java.util.Random;

public class MyRandomUtil {
    public static long getRandTime() {
        long l = System.currentTimeMillis() / 1000;
        return l;
    }

    public static String getRandStr() {
        String s = RandomStringUtils.randomAlphanumeric(5);
        return s;
    }

    public static ByteString getRandByteStr() {
        ByteString bytes = null;
        String s = RandomStringUtils.randomAlphanumeric(5);
        ByteString byteString = bytes.copyFromUtf8(s);
        return byteString;
    }

    public static double getRandTDouble() {
        double d = Math.random() * 180;
        return d;
    }

    public static String getRandBaseStationDeviceId() {
        /*String[] deviceTypes = {"test_d1","test_d2","test_d3","test_d4","test_d5","test_d6","test_d7","test_d8","test_d9","test_d10"};
        Random random = new Random();
        int i = random.nextInt(deviceTypes.length);
        return deviceTypes[i];*/

        Random random = new Random();
        int i = random.nextInt(2000) + 1;
        String deviceId = "d" + i;
        return deviceId;

    }

    public static int getRandInt() {
        Random random = new Random();
        int i = random.nextInt(1000) + 1;
        return i;
    }

    public static String getRandMACDeviceId() {
        /*String[] deviceTypes = {"test_w1","test_w2","test_w3","test_w4","test_w5","test_w6","test_w7","test_w8","test_w9","test_w10"};
        Random random = new Random();
        int i = random.nextInt(deviceTypes.length);
        return deviceTypes[i];*/

        Random random = new Random();
        int i = random.nextInt(2000) + 1;
        String deviceId = "w" + i;
        return deviceId;
    }

    public static String getRandVehicleDeviceId() {
        /*String[] deviceTypes = {"test_k1","test_k2","test_k3","test_k4","test_k5","test_k6","test_k7","test_k8","test_k9","test_k10"};
        Random random = new Random();
        int i = random.nextInt(deviceTypes.length);
        return deviceTypes[i];*/

        Random random = new Random();
        int i = random.nextInt(2000) + 1;
        String deviceId = "k" + i;
        return deviceId;
    }
}
