package aim.iot.iotnode;


import org.fusesource.hawtbuf.Buffer;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.Future;
import org.fusesource.mqtt.client.FutureConnection;
import org.fusesource.mqtt.client.Promise;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Tracer;


import org.fusesource.mqtt.client.Topic;
import org.fusesource.mqtt.client.Callback;
import org.fusesource.mqtt.client.CallbackConnection;
import org.fusesource.mqtt.client.Listener;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.codec.MQTTFrame;


import java.net.URISyntaxException;

/**
 * Created by amaarala on 8/20/14.
 */
public class MQTTProducer {

    final Promise<Buffer> result = new Promise<Buffer>();
    static String brokerAddress;
    static String topic;
    FutureConnection connection = null;
    static Topic[] topics = null;


    public MQTTProducer(String brokerAddress, final String topic){

        this.brokerAddress = brokerAddress;
        this.topic = topic;
        topics = new Topic[]{new Topic(topic, QoS.AT_LEAST_ONCE)};

     try {

        MQTT mqtt = new MQTT();
        try {
            //mqtt.setHost("10.20.210.228", 10061);
            mqtt.setHost(brokerAddress);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }


        mqtt.setTracer(new Tracer(){
            @Override
            public void onReceive(MQTTFrame frame) {
                System.out.println("MQTTProducer recv: "+frame);
            }
            @Override
            public void onSend(MQTTFrame frame) {
                System.out.println("MQTTProducer send: "+frame);
            }
            @Override
            public void debug(String message, Object... args) {
                System.out.println(String.format("MQTTProducer debug: "+message, args));
            }
        });

        connection = mqtt.futureConnection();

        Future<Void> f1 = connection.connect();
        f1.await();
        //Future<byte[]> f2 = connection.subscribe(topics);
        //byte[] subresult = f2.await();




        /*try {
            connection.connect();
        } catch (Exception e1) {
            e1.printStackTrace();
        }


        try {
            connection.publish(topic, "HelloConsumer!".getBytes(), QoS.AT_LEAST_ONCE, false);
        } catch (Exception e) {
            e.printStackTrace();
        }*/
       /*connection = mqtt.callbackConnection();


        connection.connect(new Callback<Void>() {
            // Once we connect..
            public void onSuccess(Void v) {
// Subscribe to a topic foo
                Topic[] topics = {new Topic(topic, QoS.AT_LEAST_ONCE)};
                connection.subscribe(topics, new Callback<byte[]>() {
                    public void onSuccess(byte[] value) {
// Once subscribed, publish a message on the same topic.
                        connection.publish(topic, "Hello".getBytes(), QoS.AT_LEAST_ONCE, false, null);
                    }
                    public void onFailure(Throwable value) {
                        result.onFailure(value);
                        connection.disconnect(null);
                    }
                });
            }
            public void onFailure(Throwable value) {
                result.onFailure(value);
            }
        });

    }
    public void onConnected() {
        System.out.println("connected");
    }

    @Override
    public void onDisconnected() {
        System.out.println("disconnected");

    }

    @Override
    public void onPublish(UTF8Buffer utf8Buffer, Buffer buffer, Runnable runnable) {
        result.onSuccess(buffer);
        //onComplete.run();
    }

    @Override
    public void onFailure(Throwable value) {
        System.out.println("failure: "+value);
        result.onFailure(value);
        connection.disconnect(null);

    }*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void produce(String s) {
        try {
            Future<Void> f3 =  connection.publish(topic, s.getBytes(), QoS.AT_LEAST_ONCE, false);
            f3.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
