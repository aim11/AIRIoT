package aim.iotsim.simulators; /**
 * Created with IntelliJ IDEA.
 * User: amaarala
 * Date: 15.10.2013
 * Time: 11:43
 * To change this template use File | Settings | File Templates.
 */
import aim.iotreasoner.IoTReasoner;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.logging.Logger;


public class XML2RDFIndivEN extends DefaultHandler{

    private Observation obs;
    private String temp;
    private ArrayList<Observation> obsList = new ArrayList<Observation>();
    private String graphIRI = "http://localhost/SensorSchema";

    public static Logger LOGGER = Logger.getLogger("Logger");

    private static IoTReasoner ioTReasoner = null;
    private static Connection c;
    Statement stmt = null;

    static int start = 1000;
    static int end = 60000;
    static int fileno = 0;

    public static void main(String[] args)
            throws IOException, SAXException, ParserConfigurationException{

        //USE SQLITEDB
        initSQLiteCon();
        //createTable();


        for(int i=start; i<end; i++){

            SAXParserFactory spfac = SAXParserFactory.newInstance();
            SAXParser sp = spfac.newSAXParser();
            XML2RDFIndivEN handler = new XML2RDFIndivEN();

            long s = new Date().getTime();

            sp.parse("/home/amaarala/thesis/parse/data/incident" + i + ".xml", handler);

            handler.serializeToRDF(i);
            //System.out.println((new Date().getTime())-s);
            //System.out.println("Incident "+i);
        }
    }

    public void characters(char[] buffer, int start, int length) {
        temp = new String(buffer, start, length);
    }


    /*
        * Every time the parser encounters the beginning of a new element,
        * it calls this method, which resets the string buffer
        */
    public void startElement(String uri, String localName,
                             String qName, Attributes attributes) throws SAXException {
        temp = "";
        if (qName.equalsIgnoreCase("Observation")) {
            obs = new Observation();
            obs.setID(Integer.parseInt(attributes.getValue("ID")));

        }

        if (qName.equalsIgnoreCase("Coordinates")) {
            double lat = Double.parseDouble(attributes.getValue("Y"));
            double lat1 = Double.parseDouble(String.valueOf(lat).substring(0, 2));
            double lat2 = (lat-(lat1*100))/60;
            double latDec = lat1+lat2;

            double lon = Double.parseDouble(attributes.getValue("X"));
            double lon1 = Double.parseDouble(String.valueOf(lon).substring(0, 2));
            double lon2 = (lon-(lon1*100))/60;
            double lonDec = lon1+lon2;

            obs.setLat(latDec);
            obs.setLon(lonDec);


        }
    }

    /*
     * When the parser encounters the end of an element, it calls this method
     */
    public void endElement(String uri, String localName, String qName)
            throws SAXException {

        if (qName.equalsIgnoreCase("Observation")) {
            // add it to the list
            obsList.add(obs);

        } else if (qName.equalsIgnoreCase("Area")) {
            obs.setArea(Integer.parseInt(temp));
        } else if (qName.equalsIgnoreCase("Sender")) {
            obs.setSender(Integer.parseInt(temp));
        } else if (qName.equalsIgnoreCase("Time")) {
                SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                Date date = null;
                try {
                    date = sf.parse(temp);
                } catch (ParseException e) {
                    //e.printStackTrace();
                }
                obs.setTimeStamp(date);

        }else if (qName.equalsIgnoreCase("Direction")) {
            obs.setDirection(Integer.parseInt(temp));
        }else if (qName.equalsIgnoreCase("Velocity")) {
            obs.setVelocity(Double.parseDouble(temp));
        }

    }



    private void readList() {
       System.out.println("No observations in dataset '" + obsList.size()  + "'.");
       Iterator<Observation> it = obsList.iterator();
        while (it.hasNext()) {
            Observation obser = it.next();
            System.out.println(obser.getTimeStamp().toString());
            System.out.println(obser.getLat());
            System.out.println(obser.getLon());
        }
    }


    private void serializeToRDF(int incident) throws FileNotFoundException {
        //Model myGraph = new LinkedHashModel(); // a collection of several RDF statements


        Date prevTimestamp = null;
        double prevVelocity = 0.0;
        double distance = 0.0;
        double prevLat = 0.0;
        double prevLon = 0.0;
        Date estimatedTime = null;
        double estimatedAcc = 0.0;

        //ValueFactory factory = ValueFactoryImpl.getInstance();
        Collections.reverse(obsList);
        Iterator<Observation> it = obsList.iterator();

        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");


        while (it.hasNext()) {

            Observation observation = it.next();


            if(observation.getID() == 1){
                estimatedTime = observation.getTimeStamp();
                if(estimatedTime == null) estimatedTime = new Date();
            }else{
                distance = this.estimateDistance(prevLat, prevLon, observation.getLat(), observation.getLon());
                estimatedTime = this.estimateTime(distance, prevVelocity, observation.getVelocity(), prevTimestamp);
                estimatedAcc = this.estimateAcceleration(distance, prevVelocity, observation.getVelocity(), prevTimestamp);
            }
            String enPacket = "[urn:uuid:7bcf39 ";
            enPacket += observation.getID()+" "+ observation.getArea() +" "+ observation.getLat() +" "+ observation.getLon() +
                    " "+ observation.getVelocity() +" "+  observation.getDirection() +" "+ observation.getSender() +" "+ distance +" "
                    + estimatedAcc +" "+ estimatedTime.getTime() +" "+ sf.format(estimatedTime);

            prevTimestamp = estimatedTime;
            prevVelocity = observation.getVelocity();
            prevLat = observation.getLat();
            prevLon = observation.getLon();

            enPacket += "]";

            if( c!=null ){

                try {
                    addStatementTOSQLite(enPacket, observation.getSender());
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }else{
                FileWriter fw = null;
                int no = start+fileno;
                /*try {
                    fw = new FileWriter("/home/amaarala/thesis/obs_data_individuals_en/incident_"+no+".en");
                    fw.write(enPacket);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                try {
                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }*/
            }
            fileno++;
        }


       /*ioTReasoner.getReasoner().initDataModel(dataModel);

        //Only listed classes are inferred from body rules
        String[] classes = {"Jam", "LongStop", "VeryLongStop", "StopOnMotorway", "HighAvgSpeed", "RightTurn", "LeftTurn", "U-Turn"};

        OntModel inferredModel = ioTReasoner.getReasoner().inferFromRules(classes);
        inferredModel.write(System.out);*/
        //ioTReasoner.getReasoner().updateSesameRepository(dataModel);

    }

    private double estimateDistance(double prevLat, double prevLon, double lat, double lon) {
        //Gives distance in meters
        double R = 6371000;
        double lon1=prevLon*Math.PI/180;
        double lon2=lon*Math.PI/180;
        double lat1=prevLat*Math.PI/180;
        double lat2=lat*Math.PI/180;

        return Math.acos(Math.sin(lat1)*Math.sin(lat2) + Math.cos(lat1)*Math.cos(lat2) * Math.cos(lon2-lon1)) * R;

    }
                  //TODO: check timestamp value, seems to be 1 month too early
    private Date estimateTime(double distance, double prevVelocity, double currVelocity, Date prevTimestamp) {
        double meanVelocity = (prevVelocity+currVelocity)/2;
        double tTime = distance/(meanVelocity/3600);     //in milliseconds
        //if null(corrupted data) just add now to timestamp
        if(prevTimestamp == null)
            prevTimestamp = new Date();
        Date estimatedTT =  prevTimestamp;
        estimatedTT.setTime(prevTimestamp.getTime()+(int)tTime);
        return  estimatedTT;
    }

    private double estimateAcceleration(double distance, double prevVelocity, double currVelocity, Date prevTimestamp) {
        double meanVelocity = (prevVelocity+currVelocity)/2;
        double deltaV = currVelocity-prevVelocity;
        double tTime = distance/(meanVelocity/3.6);     //in seconds

        return  (deltaV/3.6)/tTime;  //in m/s^2
    }

    public static void initSQLiteCon(){



        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:observations_en.db");
            c.setAutoCommit(false);

            System.out.println("Opened database successfully");


        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            System.exit(0);
        }

    }

    public static void createTable(){


        Statement stmt = null;
        try {
            stmt = c.createStatement();

            String sql = "CREATE TABLE observations " +
                    "(id INTEGER PRIMARY KEY AUTOINCREMENT, sender INTEGER," +
                    " rdf           TEXT    NOT NULL)";
            stmt.executeUpdate(sql);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void addStatementTOSQLite(String rdf, int sender) throws SQLException {

        if(sender != 0 && rdf != null){
            String text =  rdf.replaceAll("\n", "");
            //String id = uuid.replaceAll("_", "").replaceAll("-", "");
            stmt = c.createStatement();
            String sql = "INSERT INTO observations (sender, rdf) VALUES('"+sender+"', '"+text+"');";
            try{
                stmt.executeUpdate(sql);
                stmt.close();
                //System.out.println("ok");

            }catch(NullPointerException ne){
                stmt.close();
                initSQLiteCon();
            }

            c.commit();


        }
    }

}

