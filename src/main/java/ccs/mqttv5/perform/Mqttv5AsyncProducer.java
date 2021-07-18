package ccs.mqttv5.perform;

import java.util.UUID;

import org.eclipse.paho.mqttv5.client.DisconnectedBufferOptions;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
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

public class Mqttv5AsyncProducer {
    private static final Logger log = LoggerFactory.getLogger(Mqttv5AsyncProducer.class);

    public static void main(String[] args) throws Exception, MqttException {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        String topic = System.getProperty("ccs.perform.topic", "test");
        String key = System.getProperty("ccs.perform.key", "defaultkey");
        int qos = Integer.getInteger("qos", 0);
        int iter = Integer.valueOf(System.getProperty("ccs.perform.iterate", "20"));
        long loop_ns = 5_000_000_000L; // ns = 5s

        String broker = CommonProperties.get("ccs.mqttv5.broker", "tcp://localhost:1883");
        String clientId = System.getProperty("ccs.perform.clientid", UUID.randomUUID().toString());

        MemoryPersistence persistence = new MemoryPersistence();
        MqttAsyncClient client = new MqttAsyncClient(broker, clientId, persistence);
        MqttConnectionOptions connOpts = new MqttConnectionOptions();
        client.setCallback(new SimpleMqttCallback() {

            @Override
            public void deliveryComplete(IMqttToken token) {
                // 配信完了をチェックするカウンタを入れるかも？
            }

        });
        connOpts.setCleanStart(true);
        DisconnectedBufferOptions bufferOpts = new DisconnectedBufferOptions();
//        bufferOpts.setBufferSize(1000);
//        client.setBufferOpts(bufferOpts);

        client.connect(connOpts).waitForCompletion();
        // XXX
        while( !client.isConnected()) {
            Thread.onSpinWait();
        };

        LatencyMeasurePingSerializer serializer = new LatencyMeasurePingSerializer();
        try {

            int seq = 0;
            // トピックを指定してメッセージを送信する
            for (int i = 0; i != iter; i++) {

                int cnt = 0;
                long st = System.nanoTime();
                long et = 0;

                while ((et = System.nanoTime()) - st < loop_ns) {
                    if( client.isConnected()) {
                        MqttMessage message = new MqttMessage(serializer.serialize(topic, new LatencyMeasurePing(seq)));
                        message.setQos(qos);
                        client.publish(topic, message);
                        seq++;
                        cnt++;
                    }else {
                    }
                }

                log.info("{}: {} ns. {} times. {} ns/op", key, et - st, cnt, (et - st) / (double) cnt);
            }
        } catch( Throwable th ) {
            th.printStackTrace();
        } finally {
            client.close(true);
        }
    }
}
