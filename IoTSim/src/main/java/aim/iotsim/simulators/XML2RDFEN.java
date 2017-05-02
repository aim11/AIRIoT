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
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.logging.Logger;

public class XML2RDFEN extends DefaultHandler{

    private Observation obs;
    private String temp;
    private ArrayList<Observation> obsList = new ArrayList<Observation>();
    private String graphIRI = "http://localhost/SensorSchema";
    private String ontologyURI = "http://localhost/SensorSchema/ontology#";


    public static Logger LOGGER = Logger.getLogger("Logger");

    private static IoTReasoner ioTReasoner = null;


    public static void main(String[] args)
            throws IOException, SAXException, ParserConfigurationException{

        ioTReasoner = new IoTReasoner();
        //ioTReasoner.setRulesFile("file:./IoTSim/rules_jena.txt");

        //ioTReasoner.initializeGenericReasoner();

        int start = 15499;
        int end = 15550;
        for(int i=start; i<end; i++){
           
            SAXParserFactory spfac = SAXParserFactory.newInstance();
            SAXParser sp = spfac.newSAXParser();
            XML2RDFEN handler = new XML2RDFEN();

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


       /* "http://www.opengis.net/ont/geosparql#Feature"/>
        </rdfs:Class><rdf:Property rdf:about="http://localhost:8890/SensorSchema#hasExactGeometry">
        <rdfs:subPropertyOf rdf:resource="http://www.opengis.net/ont/geosparql#hasGeometry"/>
        <rdfs:subPropertyOf rdf:resource="http://www.opengis.net/ont/geosparql#hasDefaultGeometry"/>
        </rdf:Property><rdf:Property rdf:about="http://localhost:8890/SensorSchema#hasPointGeometry">
        <rdfs:subPropertyOf rdf:resource="http://www.opengis.net/ont/geosparql#hasGeometry"/>
                */
        /*
        * ssn:featureOfInterest = taxi
        * */

        String baseSSNURI =  "http://purl.oclc.org/NET/ssnx/ssn#";
        String baseGeoSparqlURI =  "http://www.opengis.net/ont/geosparql#";
        String baseOGCURI =  "http://www.opengis.net/ont/sf#";
        String wgsURI =  "http://www.w3.org/2003/01/geo/wgs84_pos#";
        String ownURI =  "http://localhost/SensorSchema#";

        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        int count = 0;

        String enPacket = "[urn:uuid:7bcf39 \n";
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

            enPacket += observation.getID()+" "+ observation.getArea() +" "+ observation.getLat() +" "+ observation.getLon() +
                    " "+ observation.getVelocity() +" "+  observation.getDirection() +" "+ observation.getSender() +" "+ distance +" "
                    + estimatedAcc +" "+ estimatedTime.getTime() +" "+ sf.format(estimatedTime)+"\n";

            //myGraph.add(factory.createStatement(owlClassURI, owlClassURI,  factory.createLiteral("GPSObservation")));
            //myGraph.add(factory.createStatement(obsURI, RDFS.SUBCLASSOF, obsType));

            prevTimestamp = estimatedTime;
            prevVelocity = observation.getVelocity();
            prevLat = observation.getLat();
            prevLon = observation.getLon();

            count++;
        }

        enPacket += "]";
        FileWriter fw = null;
        try {
            fw = new FileWriter("/home/amaarala/thesis/obs_data_en/incident_"+incident+".en");
            fw.write(enPacket);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //LOGGER.info("Obs count: "+count);




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

    private String getTemplate(){
        String obsTemplate = "["+ontologyURI+"Observation "+ ontologyURI + "hasID "+ontologyURI + "hasArea "+ontologyURI +
          "hasLatitude "+ontologyURI + "hasLongitude "+ontologyURI + "hasVelocity "+ontologyURI + "hasDirection "+ontologyURI +
          "hasSender "+ontologyURI + "hasDistance "+ontologyURI + "hasAcceleration "+ontologyURI + "hasDate "+ontologyURI + "hasDateTime]";
        return obsTemplate;
    }


}

