package com.example.mq.common;

import org.apache.ibatis.javassist.bytecode.ByteArray;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * Description: 字节数组的序列化和反序列化的通用思路（其它Java对象也可以）
 * 要想对象可以进行序列化或反序列化，需要实现标准库的Serializable接口
 * User: 12569
 * Date: 2024-03-10
 * Time: 23:17
 */
public class BinaryTool {
    /**
     * 把一个对象序列化为一个字节数组
     * @param object
     * @return
     */
    public static byte[] toBytes(Object object) throws IOException {
        // 该流对象相当于一个变长的字节数组。
        // 可以把 object 序列化的数据逐渐写入到该对象中，再统一转成字节数组
        // 可以解决字节数组长度不确定的问题
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {   // 关联 byteArrayOutputStream
                // writeObject 会把该对象进行序列化，生成的二进制字节数据写入到 ObjectOutputStream 中
                // 由于 ObjectOutputStream 关联了 byteArrayOutputStream ，所以最终结果会写入到 byteArrayOutputStream 中
                objectOutputStream.writeObject(object);
            }
            // 把 byteArrayOutputStream 持有的二进制数据取出，转成 byte[]
            return byteArrayOutputStream.toByteArray();
        }
    }

    /**
     * 把一个字节数组反序列化为一个对象
     * @param data
     * @return
     */
    public static Object fromBytes(byte[] data) throws IOException, ClassNotFoundException {
        Object object = null;
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data)) {
            try (ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
                // 从data这个字节数组读取数据并进行反序列化
                object = objectInputStream.readObject();
            }
        }
        return object;
    }
}
