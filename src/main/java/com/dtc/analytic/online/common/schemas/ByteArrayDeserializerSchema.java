package com.dtc.analytic.online.common.schemas;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

public class ByteArrayDeserializerSchema<T> extends AbstractDeserializationSchema<byte[]> {

    @Override
    public byte[] deserialize(byte[] bytes) throws IOException {
        return bytes;
    }
}
