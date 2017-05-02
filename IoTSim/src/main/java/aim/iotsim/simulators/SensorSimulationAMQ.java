package aim.iotsim.simulators;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.jms.Connection;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.*;
import java.util.Date;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: amaarala
 * Date: 23.1.2014
 * Time: 14:49
 * To change this template use File | Settings | File Templates.
 */
public class SensorSimulationAMQ {


    static long reasoningLatency = 0;

    private static final Logger LOGGER = LoggerFactory
            .getLogger(SensorSimulationAMQ.class);
    static int incident = 15000;
    static int noIncs = 100;
    static String format = "rdf";


    private static java.sql.Connection c;
    Statement stmt = null;

    private static final String clientQueueName;
    private static final int ackMode;

    static {
        clientQueueName = "events";
        ackMode = Session.AUTO_ACKNOWLEDGE;
    }

    static ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://cse-cn0004:10012");
    static Connection connection;
    private static Session session;
    private static Destination destination;

    public static void main(String[] args){
        System.out.println("Start params:");
	    for (String s: args) {
            String opt = s.substring(0,2);
            if(opt.equals("-f"))
                format = s.substring(2);
                System.out.println("Format: "+format);
            if(opt.equals("-o")){
                noIncs = Integer.parseInt(s.substring(2));
                System.out.println("No incidents: "+noIncs);
            }

        }

        try {
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, ackMode);
            destination = session.createTopic(clientQueueName);
        } catch (JMSException e) {
            e.printStackTrace();
        }

        int noThreads = 1;

        Latch l = new Latch(noThreads);
        LOGGER.info("Starting client threads: " + (new Date()).toString());
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

    static class JobSlice extends Thread {
        private Latch latch;
        private int n;
        private MessageProducer producer;

        public JobSlice(Latch l, int i) {
            this.n=i;
            this.latch = l;

        }
        public void run() {
            try {
                Random random = new Random(System.currentTimeMillis());
                long randomLong = random.nextLong();
                String corrid =  Long.toHexString(randomLong);

                try {
                    this.producer = session.createProducer(destination);
                    this.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                } catch (JMSException e) {
                    e.printStackTrace();
                }

                //System.out.println("Thread: " + getName() + " running");
                FileInputStream inputStream = null;

                try {


                    for (int j = 1; j <= noIncs; j++) {
                        int inc = incident+n*noIncs+j-noIncs;
                        inputStream = new FileInputStream("/home/amaarala/thesis/data/obs_data_individuals_"+format+"/incident_"+inc+"."+format);

                        StringWriter strw = new StringWriter();
                        String encoding = "UTF-8";

                        try {
                            IOUtils.copy(inputStream, strw, encoding);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        finally {
                            try {
                                inputStream.close();
                            } catch (IOException e) {
                                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                            }
                        }

                        String rdfData = strw.toString();

                        TextMessage txtMessage = null;
                        try {
                            txtMessage = session.createTextMessage();
                            txtMessage.setText(rdfData);
                            txtMessage.setJMSCorrelationID(corrid);
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

                } catch (FileNotFoundException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }

            } finally {
                latch.countDown();
            }
        }
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
                    System.out.println("Reasoning latency:  "+reasoningLatency);
                    //TODO: post reasoning can be done here, get triples from database and reason
                }
            }
        }

        public static void initSQLiteCon(){



            try {
                Class.forName("org.sqlite.JDBC");
                c = DriverManager.getConnection("jdbc:sqlite:observations_json.db");
                c.setAutoCommit(false);

                System.out.println("Opened database successfully");


            } catch ( Exception e ) {
                System.err.println( e.getClass().getName() + ": " + e.getMessage() );
                System.exit(0);
            }

        }

        public String getObservation(int id) throws SQLException {

                String sql = "SELECT rdf FROM observations WHERE id='"+id+"'";
                Statement stmt = c.createStatement();
            String rdf = null;
                try{
                    stmt.executeQuery(sql);
                    ResultSet rs = stmt.getResultSet();
                    rdf = rs.getString(0);
                    stmt.close();
                    //System.out.println("ok");

                }catch(NullPointerException ne){
                    stmt.close();
                    initSQLiteCon();
                }


            return rdf;
        }
    }
}
