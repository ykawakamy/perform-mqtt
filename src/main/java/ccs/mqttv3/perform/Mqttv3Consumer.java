package ccs.mqttv3.perform;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ccs.mqtt.data.LatencyMeasurePing;
import ccs.mqtt.data.LatencyMeasurePingDeserializer;
import ccs.perform.util.CommonProperties;
import ccs.perform.util.PerformCounterMap;
import ccs.perform.util.PerformHistogram;
import ccs.perform.util.PerformSnapshot;

public class Mqttv3Consumer {
    /** ロガー */
    private static final Logger log = LoggerFactory.getLogger(Mqttv3Consumer.class);

    public static void main(String[] args) throws Exception {
//        SLF4JBridgeHandler.removeHandlersForRootLogger();
//        SLF4JBridgeHandler.install();

        String topic = System.getProperty("ccs.perform.topic", "test");
        String groupid = System.getProperty("ccs.perform.groupid", "defaultgroup");
        int qos = Integer.getInteger("qos", 1);
        String key = System.getProperty("ccs.perform.key", "defaultkey");
        long loop_ns = 5_000_000_000L; // ns = 5s
        int iter = Integer.valueOf(System.getProperty("ccs.perform.iterate", "20"));

        String broker = CommonProperties.get("ccs.mqttv3.broker", "tcp://localhost:1883");
        String clientId = System.getProperty("ccs.perform.clientid", UUID.randomUUID().toString());

        PerformHistogram hist = new PerformHistogram();
        hist.addShutdownHook();

        PerformCounterMap pcMap = new PerformCounterMap();

        LatencyMeasurePingDeserializer serializer = new LatencyMeasurePingDeserializer();

        //
        MemoryPersistence persistence = new MemoryPersistence();
        MqttClient client = new MqttClient(broker, clientId, persistence);
        MqttConnectOptions connOpts = new MqttConnectOptions();
        client.setCallback(new SimpleMqttCallback() {

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                LatencyMeasurePing ping = serializer.deserialize(topic, message.getPayload());
                long latency =ping.getLatency();
                hist.increament(latency);
                pcMap.perform(topic, ping.getSeq());
                pcMap.addLatency(topic, latency);
            }

        });
        connOpts.setCleanSession(true);

        client.connect(connOpts);
        client.subscribe(topic, qos);

        try {
            // トピックを指定してメッセージを送信する
            for( int i=0 ; i != iter ; i++ ) {
                long st = System.nanoTime();
                long et = 0;
                TimeUnit.NANOSECONDS.sleep(loop_ns);
                et = System.nanoTime();

                PerformSnapshot snap = pcMap.reset();
                snap.print(log, et-st);
            }
        } catch( Throwable th ) {
            th.printStackTrace();
        } finally {
            client.disconnect();
            client.close(true);

        }
    }
}
