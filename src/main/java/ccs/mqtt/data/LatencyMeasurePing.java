
package ccs.mqtt.data;

public class LatencyMeasurePing {
    int seq;
    long sendTick;
    long latency;

    public LatencyMeasurePing(int seq) {
        this.seq = seq;
        // Note : System.nanoTime()はJVMごとで別の値を取るため。
        sendTick = System.currentTimeMillis();
    }

    public LatencyMeasurePing(int seq, long tick) {
        this.seq = seq;
        this.sendTick = tick;
        this.latency =  System.currentTimeMillis() - tick;
    }

    public int getSeq() {
        return seq;
    }

    public long getLatency() {
        return latency;
    }

}
