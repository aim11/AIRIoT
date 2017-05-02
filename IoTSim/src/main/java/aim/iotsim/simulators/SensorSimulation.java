package aim.iotsim.simulators;

import aim.iotreasoner.IoTReasoner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: amaarala
 * Date: 23.1.2014
 * Time: 14:49
 * To change this template use File | Settings | File Templates.
 */
public class SensorSimulation {

    private static String owlFile = "/home/amaarala/thesis/dev/IoT/IoTSim/ontology.owl";
    private static String ruleFile = "file:/home/amaarala/thesis/dev/IoT/IoTSim/rules_selective2.txt";

    static long reasoningLatency = 0;

    static int incident = 15000;
    static int noIncs = 10;

    private static final Logger LOGGER = LoggerFactory
            .getLogger(SensorSimulation.class);

    public static void main(String[] args){

        int noThreads = 10;
        long startTime =  (new Date()).getTime();
        Latch l = new Latch(noThreads);
        LOGGER.info("Starting client threads: " +startTime);
        for (int i = 0; i < noThreads; i++) {
            Thread j = new JobSlice(l,i);
            j.start();
        }
        try {
            l.awaitZero();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

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


                    for (int j = 0; j < 10; j++) {
                        int inc = incident+n*noIncs+j-1;
                        inputStream = new FileInputStream("/home/amaarala/thesis/obs_data_rdf_n3/incident_"+inc+".n3");

                        IoTReasoner ioTReasoner = new IoTReasoner(owlFile, "http://localhost/SensorSchema/ontology#", "obs", ruleFile);
                        ioTReasoner.setDataFormat("N3");

                        ioTReasoner.createDataModel(inputStream);
                        //Only listed types are inferred from body rules
                        String[] types = {"RightTurn", "LeftTurn", "UTurn", "Jam", "HighAvgSpeed", "LongStop"};
                        //long time = (new Date()).getTime();
                        ioTReasoner.inferModel(types, true);

                     }

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
        private long startTime;

        public Latch(int noThreads) {
            startTime =  (new Date()).getTime();
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

                    System.out.println((new Date()).getTime() - startTime);
                    //TODO: post reasoning can be done here, get triples from database and reason
                }
            }
        }
    }
}
