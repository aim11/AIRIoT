package aim.iot.iotnode;

import android.content.Context;
import android.util.Log;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import com.hp.hpl.jena.rdf.model.Model;

//import org.fusesource.hawtbuf.Buffer;
//import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.Future;
import org.fusesource.mqtt.client.FutureConnection;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.Promise;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Tracer;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.Topic;
import org.fusesource.mqtt.client.Callback;
import org.fusesource.mqtt.client.CallbackConnection;
import org.fusesource.mqtt.client.Listener;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.codec.MQTTFrame;


import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import aim.iot.iotnode.reasoner.IoTReasoner;

/**
 * Created by amaarala on 8/20/14.
 */
public class MQTTClient  {

    static String brokerAddress;
    static String topic;
    static Topic[] topics = null;
    static String msgPool = "";
    static int noTriples = 1200;

    public static void subThredCount() {
        thredCount = thredCount-1;
    }

    static int thredCount = 0;

    public static FutureConnection fconnection;

    //static BlockingConnection connection;
    private static boolean subscribed = false;


    public MQTTClient(String brokerAddress, int noTriples, String topic, String ontology, String rules) {

        this.brokerAddress = brokerAddress;
        this.topic = topic;
        topics = new Topic[]{new Topic(topic, QoS.AT_LEAST_ONCE)};
        this.noTriples = noTriples;

        //reasoner = new IoTReasoner();
        //reasoner.setOntology(ontology);
        //reasoner.setRules(rules);
        //reasoner.initializeGenericReasoner();

        thread(new Consumer(ontology, rules), false);

    }


    public static void thread(Runnable runnable, boolean daemon) {
        thredCount = thredCount+1;
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();

    }

    public static class Consumer implements Runnable {


        private GPSObservationModel serverdataModel;
        private String ontology;
        private String rules;

        public Consumer(String ontology, String rules) {
            this.ontology = ontology;
            this.rules = rules;
        }

        public void run() {
            //final Promise<Buffer> result = new Promise<Buffer>();

            if(serverdataModel == null)
                serverdataModel = new GPSObservationModel();

            try {
                MQTT mqtt = new MQTT();
                try {
                    mqtt.setHost(brokerAddress);
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                }

                mqtt.setTracer(new Tracer(){

                    @Override
                    public void onReceive(MQTTFrame frame) {

                        if(subscribed == true)
                        try {

     /*                       Message message = connection.receive();
                            String payload = new String(message.getPayload());
                            //serverdataModel.addENtoDataModel(payload);
                            //RDF/XML

                            //StringReader reader = new StringReader(payload);
                            //serverdataModel.getDataModel().read(reader, null, "RDF/XML");

                            serverdataModel.addENtoDataModel(payload);

                            message.ack();
                            if (serverdataModel.getDataModel().size()>noTriples){
                                //System.out.println("Starting reasoning: " +serverdataModel.getDataModel().size());
                                thread(new Reasoner(serverdataModel, ontology, rules), false);

                                serverdataModel = new GPSObservationModel();
                            }
*/

                            Future<Message> receive = fconnection.receive();
                            if( thredCount < 3 ){
                               receive.then(new Callback<Message>() {

                                    @Override
                                    public void onSuccess(Message message) {
                                        String payload = new String(message.getPayload());
                                        //serverdataModel.addENtoDataModel(payload);
                                        //RDF/XML

                                        //StringReader reader = new StringReader(payload);
                                        //serverdataModel.getDataModel().read(reader, null, "RDF/XML");

                                        serverdataModel.addENtoDataModel(payload);

                                        message.ack();
                                        if (serverdataModel.getDataModel().size()>noTriples){
                                            //System.out.println("Starting reasoning: " +serverdataModel.getDataModel().size());
                                            thread(new Reasoner(serverdataModel, ontology, rules), false);

                                            serverdataModel = new GPSObservationModel();
                                        }

                                    }

                                    @Override
                                    public void onFailure(Throwable throwable) {
                                        System.out.println("GOSH! Receiving msg Failed!"+ throwable.getMessage());
                                        Reasoning.updateUI("GOSH! Receiving msg Failed!"+ throwable.getMessage());
                                    }
                                });
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                    @Override
                    public void onSend(MQTTFrame frame) {
                        //System.out.println("consumer send: "+frame);
                    }
                    @Override
                    public void debug(String message, Object... args) {
                        System.out.println(String.format("consumer debug: "+message, args));
                        Reasoning.updateUI("Consumer: "+message);
                    }
                });

/*                connection = mqtt.blockingConnection();

                try {
                    connection.connect();
                } catch (Exception e1) {
                    e1.printStackTrace();
                }*/


                fconnection = mqtt.futureConnection();
                Future<Void> f1 = fconnection.connect();

                f1.await(10, TimeUnit.SECONDS);

/*
                f1.then(new Callback<Void>() {
                    // Once we connect..
                    public void onSuccess(Void v) {
                        // Subscribe to a topic foo

                        try {
                            Future<byte[]> subs = fconnection.subscribe(topics);
                            subs.await(10, TimeUnit.SECONDS);

                            Reasoning.updateUI("Consumer subscribed to topic!");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }

                    public void onFailure(Throwable value) {
                        Reasoning.updateUI("Connection failure!");
                        value.printStackTrace();
                    }
                });*/
                //thread(new ConsumingListener(), false);



// We can start future receive..




                //Future<Void> f4 = fconnection.disconnect();
                //f4.await();

                //BLOCKING CONNECTION
/*                connection = mqtt.blockingConnection();
                try {
                    connection.connect();
                } catch (Exception e1) {
                    e1.printStackTrace();
                }



                connection.subscribe(topics);

                connection.publish(topic, "HelloY!".getBytes(), QoS.AT_LEAST_ONCE, false);

*/
/*
                final CallbackConnection cbconnection = mqtt.callbackConnection();

                cbconnection.connect(new Callback<Void>() {
                    // Once we connect..
                    public void onSuccess(Void v) {
                        // Subscribe to a topic foo

                        cbconnection.subscribe(topics, new Callback<byte[]>() {
                            public void onSuccess(byte[] value) {
                                // Once subscribed, publish a message on the same topic.
                                cbconnection.publish(topic, "Hello".getBytes(), QoS.AT_LEAST_ONCE, false, null);
                            }

                            public void onFailure(Throwable value) {
                                result.onFailure(value);
                                //cbconnection.disconnect(null);
                            }
                        });

                    }

                    public void onFailure(Throwable value) {
                        result.onFailure(value);
                    }
                });

                cbconnection.listener(new Listener() {

                    public void onDisconnected() {
                        System.out.println("Disconnected");
                    }
                    public void onConnected() {
                        System.out.println("Connected");
                    }

                    public void onPublish(UTF8Buffer topic, Buffer payload, Runnable ack) {
                        // You can now process a received message from a topic.
                        // Once process execute the ack runnable.
                        System.out.println("FFF"+topic.toString()+" "+payload.toString());
                        ack.run();
                    }
                    public void onFailure(Throwable value) {
                        cbconnection.disconnect(null); // a connection failure occured.
                    }
                });
                cbconnection.publish("sensor.events", "Hello".getBytes(), QoS.AT_LEAST_ONCE, false, null);
*/

            } catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
             }
        }


        /*public static class ConsumingListener implements Runnable {
            public void run() {

                try {
                    fconnection.subscribe(topics).await();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                Future<Message> receive = fconnection.receive();

                receive.then(new Callback<Message>() {

                    @Override
                    public void onSuccess(Message message) {
                        String payload = new String(message.getPayload());
                        msgPool = msgPool +" "+ payload;
                        System.out.println("This is future! says consumer: "+msgPool);
                        message.ack();
                    }

                    @Override
                    public void onFailure(Throwable throwable) {
                        System.out.println("GOSH! Receiving msg Failed!"+ throwable.getMessage());

                    }
                });

            }
        }*/

    }
    public void subscribe(){

            try {
                Reasoning.updateUI("Subscribing...");
                if(fconnection != null){
                    fconnection.subscribe(topics).await();
                    subscribed = true;
                    Reasoning.updateUI("Subscribed to topic!");
                }
                else {
                    Reasoning.updateUI("No connection!");
                    subscribed = false;
                }

            } catch (Exception e) {
                subscribed = false;
                e.printStackTrace();
            }
      }

}
