package ccs.mqttv5.perform;

import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

public class SimpleMqttCallback implements MqttCallback {

    @Override
    public void disconnected(MqttDisconnectResponse disconnectResponse) {
    }

    @Override
    public void mqttErrorOccurred(MqttException exception) {

    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
    }

    @Override
    public void deliveryComplete(IMqttToken token) {
    }

    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
    }

    @Override
    public void authPacketArrived(int reasonCode, MqttProperties properties) {
    }

}
