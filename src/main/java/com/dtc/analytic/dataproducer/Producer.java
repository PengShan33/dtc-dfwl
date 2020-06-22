package com.dtc.analytic.dataproducer;


import com.dtc.analytic.dataproducer.utils.KafkaProducerUtils;
import com.dtc.analytic.dataproducer.utils.ProtoUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.ExecutionException;

public class Producer {
    public static void main(String[] args) throws InterruptedException {
        String topic1 = "access_vehicle_features_test01";
        String topic2 = "access_wifi_test01";
        String topic3 = "access_basestation_test01";
        String topic4 = "access_camera_test01";

        // 创建生产者
        KafkaProducer<String, byte[]> byteProducer = KafkaProducerUtils.buildByteProducer();
        KafkaProducer<String, String> stringProducer = KafkaProducerUtils.buildStringProducer();

        // 创建消息
        while (true) {
            byte[] vehicleMessage = ProtoUtil.getVehicleMessage();
            byte[] wifiMessage = ProtoUtil.getWifiMessage();
            byte[] baseStationMessage = ProtoUtil.getBaseStationMessage();
//            String cameraMessage = ProtoUtil.getCameraMessage();

            ProducerRecord<String, byte[]> record1 = new ProducerRecord<>(topic1, vehicleMessage);
            ProducerRecord<String, byte[]> record2 = new ProducerRecord<>(topic2, wifiMessage);
            ProducerRecord<String, byte[]> record3 = new ProducerRecord<>(topic3, baseStationMessage);
//            ProducerRecord<String, String> record4 = new ProducerRecord<>(topic4, cameraMessage);

            try {
                byteProducer.send(record1).get();
                byteProducer.send(record2).get();
                byteProducer.send(record3).get();
//                stringProducer.send(record4).get();
            } catch (ExecutionException | InterruptedException e) {
                System.out.println("写入kafka异常");
                e.printStackTrace();
            }
            System.out.println("===========数据写入成功=============");
            Thread.sleep(1000 * 10);
        }
    }
}
