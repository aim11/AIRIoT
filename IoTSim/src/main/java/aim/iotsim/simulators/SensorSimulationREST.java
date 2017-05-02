package aim.iotsim.simulators;

import aim.iotreasoner.IoTReasoner;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: amaarala
 * Date: 23.1.2014
 * Time: 14:49
 * To change this template use File | Settings | File Templates.
 */
public class SensorSimulationREST {

    static long reasoningLatency = 0;

    static int incident = 15000;
    static int noIncs = 46;

    private static final Logger LOGGER = LoggerFactory
            .getLogger(IoTReasoner.class);
    private static long starttime;

    public static void main(String[] args){

        //Number of client threads
        int noThreads = 10;


        Latch l = new Latch(noThreads);
        starttime = (new Date()).getTime();
        LOGGER.info("Starting client threads: " + starttime);

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
        public JobSlice(Latch l, int i) {
            this.n=i;
            this.latch = l;

        }
        public void run() {
            try {
                //System.out.println("Thread: " + getName() + " running");
                FileInputStream inputStream = null;

                try {

                    for (int j = 1; j <= noIncs; j++) {

                        int inc = incident+n*noIncs+j-1;
                        inputStream = new FileInputStream("/home/amaarala/thesis/obs_data_rdf_n3/incident_"+inc+".n3");

                        //System.out.println("Read from file: "+inc+".rdf");

                        //Reason over datamodel
                        //String[] types = {"RightTurn", "LeftTurn", "UTurn", "Jam", "HighAvgSpeed", "LongStop", "HighAcceleration", "HighDeacceleration"};


                        //Instantiate an HttpClient
                        //HttpPost httpPost = new HttpPost("http://localhost:8888/NDaHood/rest/ERS/infer-rt?store=true&type=LongStop");
                        HttpPost httpPost = new HttpPost("http://localhost:8888/NDaHood/rest/SMS/infer-nonselective?store=true&format=N3");
                        //HttpPost httpPost = new HttpPost("http://localhost:8888/NDaHood/rest/ERS/store");

                        //HttpPost httpPost = new HttpPost("http://localhost:8888/NDaHood/rest/SMS/infer-qt");

                        //HttpPost httpPost = new HttpPost("http://localhost:8888/NDaHood/rest/SMS/infer-nonselective-en");

                        InputStreamEntity entity = new InputStreamEntity(inputStream, ContentType.create("application/rdf+xml"));

                        httpPost.setEntity(entity);

                        CloseableHttpClient httpclient = HttpClients.createDefault();
                        try {
                            CloseableHttpResponse response2 = httpclient.execute(httpPost);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }


                    }
                    //reasoningLatency = reasoningLatency + ms;
                    // System.out.println("Reasoning done. From source "+incident+".rdf");


                } catch (FileNotFoundException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
                finally {
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
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
                while (count > 0) {
                    synchObj.wait();
                }
            }
        }
        public void countDown() {
            synchronized (synchObj) {
                if (--count <= 0) {
                    synchObj.notifyAll();
                    System.out.println("Reasoning latency:  "+((new Date()).getTime()-starttime));
                    //TODO: post reasoning can be done here, get triples from database and reason
                }
            }
        }
    }
}
