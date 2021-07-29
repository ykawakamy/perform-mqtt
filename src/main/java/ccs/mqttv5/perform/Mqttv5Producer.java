package ccs.mqttv5.perform;

import java.util.UUID;

import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import ccs.mqtt.data.LatencyMeasurePing;
import ccs.mqtt.data.LatencyMeasurePingSerializer;
import ccs.perform.util.CommonProperties;
import ccs.perform.util.TopicNameSupplier;

public class Mqttv5Producer {
    private static final Logger log = LoggerFactory.getLogger(Mqttv5Producer.class);

    public static void main(String[] args) throws Exception, MqttException {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        String topicprefix = System.getProperty("ccs.perform.topic", "test");
        String topicrange = System.getProperty("ccs.perform.topicrange", null);
        String key = System.getProperty("ccs.perform.key", "defaultkey");
        int qos = Integer.getInteger("qos", 2);
        int iter = Integer.valueOf(System.getProperty("ccs.perform.iterate", "20"));
        long loop_ns = 5_000_000_000L; // ns = 5s

        String broker = CommonProperties.get("ccs.mqttv5.broker", "tcp://localhost:1883");
        String clientId = System.getProperty("ccs.perform.clientid", UUID.randomUUID().toString());

        MemoryPersistence persistence = new MemoryPersistence();
        MqttClient client = new MqttClient(broker, clientId, persistence);
        MqttConnectionOptions connOpts = new MqttConnectionOptions();
        client.setCallback(new SimpleMqttCallback() {

            @Override
            public void deliveryComplete(IMqttToken token) {
                // 配信完了をチェックするカウンタを入れるかも？
            }

        });
        connOpts.setCleanStart(true);

        client.connect(connOpts);

        TopicNameSupplier supplier = TopicNameSupplier.create(topicprefix, topicrange);

        LatencyMeasurePingSerializer serializer = new LatencyMeasurePingSerializer();
        try {

            int seq = 0;
            // トピックを指定してメッセージを送信する
            for (int i = 0; i != iter; i++) {

                int cnt = 0;
                long st = System.nanoTime();
                long et = 0;


                while ((et = System.nanoTime()) - st < loop_ns) {
                    String topic = supplier.get();
                    MqttMessage message = new MqttMessage(serializer.serialize(topic, new LatencyMeasurePing(seq)));
                    message.setQos(qos);
                    client.publish(topic, message);
                    seq++;
                    cnt++;
                }

                log.info("{}: {} ns. {} times. {} ns/op", key, et - st, cnt, (et - st) / (double) cnt);
            }

        } finally {
            client.disconnect();
            client.close(true);
        }
    }
}
