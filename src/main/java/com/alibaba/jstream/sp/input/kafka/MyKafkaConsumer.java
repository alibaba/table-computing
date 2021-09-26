package com.alibaba.jstream.sp.input.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Properties;

public class MyKafkaConsumer {
    private static class MyConsumerConfig extends ConsumerConfig {
        public MyConsumerConfig(Map<String, Object> props, boolean doLog) {
            super(props, doLog);
        }
    }

    public static KafkaConsumer newKafkaConsumer(Properties properties) {
        try {
            Constructor<KafkaConsumer> constructor = KafkaConsumer.class.getDeclaredConstructor(ConsumerConfig.class, Deserializer.class, Deserializer.class);
            constructor.setAccessible(true);
            return constructor.newInstance(new MyConsumerConfig(Utils.propsToMap(properties), false), null, null);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}
