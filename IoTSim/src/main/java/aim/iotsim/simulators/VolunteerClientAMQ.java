package aim.iotsim.simulators;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.FileInputStream;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: amaarala
 * Date: 23.1.2014
 * Time: 14:49
 * To change this template use File | Settings | File Templates.
 */
public class VolunteerClientAMQ {

    private static String owlFile = "/home/amaarala/thesis/dev/IoT/IoTSim/traffic.owl";
    private static String ruleFile = "file:/home/amaarala/thesis/dev/IoT/IoTSim/traffic.rules";

    static long reasoningLatency = 0;

    private static final Logger LOGGER = LoggerFactory
            .getLogger(VolunteerClientAMQ.class);
    static int incident = 1;
    static int noIncs = 1000;
    static int noThreads = 1;


    private static java.sql.Connection c;

    private static final String clientQueueName;
    private static final int ackMode;

    static {
        clientQueueName = "obs.queue_rdfxml";
        ackMode = Session.AUTO_ACKNOWLEDGE;
    }


    private static long start;

    static class JobSlice extends Thread {
        private static String database = "jdbc:sqlite:observations_rdfxml.db";
        private Latch latch;
        private int n;
        private MessageProducer producer;

        public JobSlice(Latch l, int i) {
            initSQLiteCon();
            this.n=i;
            this.latch = l;

        }
        public void run() {

            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://cse-cn0006:10066");
            Connection connection = null;
            Session session = null;
            Destination destination = null;


            try {
                connection = connectionFactory.createConnection();
                connection.start();

            } catch (JMSException e) {
                e.printStackTrace();
            }

            try {
                Random random = new Random(System.currentTimeMillis());
                long randomLong = random.nextLong();
                String corrid =  Long.toHexString(randomLong);

                //System.out.println("Thread: " + getName() + " running");
                FileInputStream inputStream = null;

                //Client
                Session csession = null;
                //Destination clientD = null;
                try {
                    csession = connection.createSession(false, ackMode);
                    //clientD = session.createQueue("sensor."+corrid);
                    MessageConsumer responseConsumer = csession.createConsumer(csession.createQueue("trafficdata"));
                    //This class will handle the messages to the temp queue as well
                    responseConsumer.setMessageListener(new EventListener());

                } catch (JMSException e) {
                    e.printStackTrace();
                }


                for (int j = 1; j <= noIncs; j++) {



                    try {
                        session = connection.createSession(false, ackMode);
                        destination = session.createQueue(clientQueueName);
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }

                   /* try {
                        //estination tempDest = session.createTemporaryQueue();

                        assert session != null;
                        this.producer = session.createProducer(destination);
                        this.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }*/

                    int inc = incident+n*noIncs+j-noIncs;

                    String rdfData = null;
                    String sender =null;
                    try {
                        ResultSet rs = getObservation(inc);
                        rdfData = rs.getString(1);
                        sender = rs.getString(2);
                        rs.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }

                    TextMessage txtMessage = null;
                    try {
                        txtMessage = session.createTextMessage();
                        txtMessage.setText(rdfData);
                        txtMessage.setJMSCorrelationID(corrid);
                        //txtMessage.setStringProperty("JMSXGroupID", sender);

                        //txtMessage.setStringProperty("test","test");
                        this.producer.send(destination, txtMessage);
                    } catch (JMSException e) {
                        e.printStackTrace();
                   }


                   /* String jmsMessageQueueName = "activemq:queue:sensor.events?preserveMessageQos=true";

                    final Map<String, Object> parameters = new HashMap<String, Object>();
                    parameters.put("TIMESTSAMP", (new Date()).toString());
                    parameters.put("JMSCorrelationID", urn);
                    parameters.put("urn", urn);
                    parameters.put("content", data);

                    this.producer.sendBodyAndHeaders(jmsMessageQueueName, "Sensor event", parameters);*/


                }

            } finally {
                latch.countDown();
            }
        }

        private ResultSet getObservation(int id) {

            String sql = "SELECT rdf, sender FROM observations WHERE id='"+id+"'";
            String rdf = null;
            Statement stmt = null;
            ResultSet rs =null;
            try {
                stmt = c.createStatement();

                try{

                     rs = stmt.executeQuery(sql);
                    //rdf = rs.getString(1);

                    //System.out.println("ok");

                }catch(NullPointerException ne){
                    stmt.close();
                    initSQLiteCon();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }


            return rs;
        }

        public static void initSQLiteCon(){


            long s = (new Date()).getTime();

            try {
                Class.forName("org.sqlite.JDBC");
                c = DriverManager.getConnection(database);
                c.setAutoCommit(false);

                //System.out.println("Opened database successfully");


            } catch ( Exception e ) {
                System.err.println( e.getClass().getName() + ": " + e.getMessage() );
                System.exit(0);
            }
            reasoningLatency += (new Date()).getTime()-s;

        }


        public void onMessage(Message message) {
            String messageText = null;
            try {
                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    messageText = textMessage.getText();
                    System.out.println("messageText = " + messageText);
                }
            } catch (JMSException e) {
                //Handle the exception appropriately
            }
        }
    }



    public static void main(String[] args){





        Latch l = new Latch(noThreads);
        start = new Date().getTime();
        LOGGER.info("Starting client threads: " + start);
        for (int i = 1; i <= noThreads; i++) {
            Thread j = new JobSlice(l,i);
            j.start();
        }
        try {
            l.awaitZero();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        /*for(int i=0; i<10; i++){
                final int n = i;
                new Thread("Sensor_"+ n){
                    public void run(){

                    }
                }.start();
            }*/


    }






    public static class Latch {
        private final Object synchObj = new Object();
        private int count;

        public Latch(int noThreads) {
            synchronized (synchObj) {
                this.count = noThreads;
            }
        }
        public void awaitZero() throws InterruptedException {
            synchronized (synchObj) {
                while (count > 1) {
                    synchObj.wait();
                }
            }
        }
        public void countDown() {
            synchronized (synchObj) {
                if (--count <= 1) {
                    synchObj.notifyAll();
                    System.out.println("DB read&send latency:  " + ((new Date()).getTime() - start));
                     //TODO: post reasoning can be done here, get triples from database and reason
                }
            }
        }


    }
}
