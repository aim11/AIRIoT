package aim.iot.iotnode;

import android.app.Activity;
import android.app.ActivityManager;
import android.content.Context;
import android.location.Criteria;
import android.location.LocationManager;
import android.location.LocationListener;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.location.Location;
import android.os.Bundle;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.concurrent.Executors;

import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;

import ch.ethz.inf.vs.californium.coap.CoAP;
import ch.ethz.inf.vs.californium.coap.LinkFormat;
import ch.ethz.inf.vs.californium.coap.MediaTypeRegistry;
import ch.ethz.inf.vs.californium.coap.Request;
import ch.ethz.inf.vs.californium.coap.Response;
import ch.ethz.inf.vs.californium.server.Server;
import ch.ethz.inf.vs.californium.server.resources.CoapExchange;
import ch.ethz.inf.vs.californium.server.resources.ResourceBase;



public class Reasoning extends Activity {

    private static final int INTERVAL = 1000 * 30;

    String locationProvider = LocationManager.PASSIVE_PROVIDER;
    private Location lastKnownLocation = null;
    TelephonyManager tm;
    public static TextView textview;
    String ontologyURI =  "http://localhost/SensorSchema/ontology#";

    private int idcount = 0;


    private MQTTClient mqttclient;
    private MQTTProducer mqttproducer;
    private InputStream isOnt;
    private InputStream isRules;

    private Date lastDate = new Date();
    private GPSObservationModel dataModel;
    private EditText address;
    private InputStream data;
    private String rules;
    private String ontology;
    private EditText noTriples;
    //private HTTPServer httpserver;
    //private CoapServer coapserver;

    private static Reasoning instance;
    private static ActivityManager am;
    //Statement typeStatement = factory.createStatement(obsURI, RDF.TYPE, obsType);
    //myGraph.add(typeStatement);



    @Override
    protected void onCreate(Bundle savedInstanceState) {

        instance = this;
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_reasoning);
        textview = (TextView) findViewById(R.id.textView);
        tm = (TelephonyManager) getSystemService(TELEPHONY_SERVICE);
        am = (ActivityManager) getSystemService(ACTIVITY_SERVICE);

        //Load static ontology&rules
        isOnt = getResources().openRawResource(R.raw.ontology);
        isRules = getResources().openRawResource(R.raw.rules_jena);
        data = getResources().openRawResource(R.raw.data);

         ontology = streamToString(isOnt);
         rules = streamToString(isRules);


        address = (EditText) findViewById(R.id.editText);
        address.setText(getString(R.string.broker_address));
        //thread(new CoapServer(), false);
        noTriples = (EditText) findViewById(R.id.editText2);

        thread(new Reasoner(data, ontology, rules), false);

        // Acquire a reference to the system Location Manager
       // initLocationListener();

       // if(mqttclient == null) mqttclient = new MQTTClient(getString(R.string.broker_address), getString(R.string.topic), isOnt, isRules);
        //if(mqttproducer == null) mqttproducer = new MQTTProducer(getString(R.string.broker_address), getString(R.string.topic));



        final Button button = (Button) findViewById(R.id.btn_produce);
        button.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {


                System.out.println("connecting to: "+address.getText().toString());
                if(mqttclient == null){
                    mqttclient = new MQTTClient(address.getText().toString(), Integer.parseInt(noTriples.getText().toString()), getString(R.string.topic), ontology, rules);
                }
                //if(mqttproducer == null) mqttproducer = new MQTTProducer(address.getText().toString(), getString(R.string.topic));

                //InputStream rdfData = getResources().openRawResource(R.raw.data);
                //if(mqttproducer != null) mqttproducer.produce("Device "+tm.getDeviceId() +" CONNECTED! ");
               /* StringWriter sw = new StringWriter();
                if(infmodel != null){
                    infmodel.write(sw);
                    mqttproducer.produce(sw.toString());
                }else mqttproducer.produce("Nothing reasoned!");*/

            }
        });
        final Button btnsubs = (Button) findViewById(R.id.button);
        btnsubs.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {

                if(mqttclient != null){
                    mqttclient.subscribe();
                }

            }
        });

    }

    public static void updateUI(final String string){


        new Thread() {
            public void run() {


                        ((Activity) instance).runOnUiThread(new Runnable() {

                            @Override
                            public void run() {
                                textview.append(string+System.getProperty ("line.separator"));
                            }
                        });


            }
        }.start();

    }

    public static int getMem(){

        return am.getLargeMemoryClass();

    }



    private void initLocationListener() {

        LocationManager locationManager = (LocationManager) this.getSystemService(LOCATION_SERVICE);

        // Define a listener that responds to location updates
        LocationListener locationListener = new LocationListener() {
            public void onLocationChanged(Location location) {
                // Called when a new location is found by the network location provider.
                /*if(isBetterLocation(location, lastKnownLocation)){

                }*/
                idcount++;

                if (lastKnownLocation == null) lastKnownLocation = location;
                Date date = new Date();

                appendDataModel(date, location);

                if (idcount > 20) {
                    idcount = 0;
                    thread(new Reasoner(dataModel, ontology, rules), false);
                    dataModel = new GPSObservationModel();
                }
                //StringWriter sw = new StringWriter();
                //dataModel.getDataModel().write(sw);

                lastDate = date;
                lastKnownLocation = location;

                //"<rdf:RDF xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#' xmlns:obs='http://localhost/SensorSchema/ontology#' > <rdf:Description rdf:about='http://localhost/SensorSchema/ontology#Observation_63796070_1_f4cfe609-ec66-46fe-8a81-dc2fd9ed64fd'> <obs:hasArea rdf:datatype='http://www.w3.org/2001/XMLSchema#int'>90066</obs:hasArea> <obs:hasVelocity rdf:datatype='http://www.w3.org/2001/XMLSchema#double'>2.0</obs:hasVelocity> <obs:hasLatitude rdf:datatype='http://www.w3.org/2001/XMLSchema#double'>64.97802166666666</obs:hasLatitude> <obs:hasDateTime>2013-04-05T13:00:00</obs:hasDateTime> <obs:hasID rdf:datatype='http://www.w3.org/2001/XMLSchema#int'>1</obs:hasID> <obs:hasSender rdf:datatype='http://www.w3.org/2001/XMLSchema#int'>63796070</obs:hasSender> <rdf:type rdf:resource='http://localhost/SensorSchema/ontology#Observation'/> <obs:hasDirection rdf:datatype='http://www.w3.org/2001/XMLSchema#int'>195</obs:hasDirection> <obs:hasDate rdf:datatype='http://www.w3.org/2001/XMLSchema#long'>1365156000000</obs:hasDate> <obs:hasDistance rdf:datatype='http://www.w3.org/2001/XMLSchema#double'>0.0</obs:hasDistance> <obs:hasAcceleration rdf:datatype='http://www.w3.org/2001/XMLSchema#double'>0.0</obs:hasAcceleration> <obs:hasLongitude rdf:datatype='http://www.w3.org/2001/XMLSchema#double'>25.56193166666667</obs:hasLongitude> </rdf:Description> </rdf:RDF>";

            }

            public void onStatusChanged(String provider, int status, Bundle extras) {
            }

            public void onProviderEnabled(String provider) {
            }

            public void onProviderDisabled(String provider) {
            }
        };

        // Register the listener with the Location Manager to receive location updates
        try{

            for(String s : locationManager.getProviders(true)){
                if(s.equals(LocationManager.GPS_PROVIDER)){
                    Log.d("", "GPS provider available:"+s);
                    locationProvider = LocationManager.GPS_PROVIDER;
                }
            }
            if(!locationProvider.equals(LocationManager.GPS_PROVIDER)){
                Criteria criteria = new Criteria();
                locationProvider = locationManager.getBestProvider (criteria, true);
                Log.d("ProviderBest: ", "ProviderBest available: "+locationProvider);
            }

            //Set update interval here
            locationManager.requestLocationUpdates(locationProvider, 1000, 5, locationListener);
            lastKnownLocation = locationManager.getLastKnownLocation(locationProvider);

        }catch(IllegalArgumentException e){
            e.printStackTrace();
        }
    }

    private void appendDataModel(Date date, Location location) {

        dataModel = new GPSObservationModel();

        double lat = location.getLatitude();
        double lon = location.getLongitude();
        double bearing = Double.valueOf(location.getBearing());
        double distance = Double.valueOf(location.distanceTo(lastKnownLocation));
        double speed = distance/((date.getTime()-lastDate.getTime())*1000);
        double altitude = location.getAltitude();

        textview.append("--- BearingToLast: " + location.bearingTo(lastKnownLocation));
        textview.append("  Bearing: " + bearing);
        textview.append("  Distance: " + distance);
        textview.append("  Speed: " + speed);
        textview.append("  Alt: " + altitude);
        textview.append("  Lon: " + lat);
        textview.append("  Lon: " + lon);

        Resource obsInstance = dataModel.getDataModel().createResource(dataModel.getGraphIRI() + "/ontology#Observation_" + locationProvider + "_" + date.getTime());

        dataModel.getDataModel().add(obsInstance, RDF.type, dataModel.getObsType());
        dataModel.getDataModel().add(obsInstance, dataModel.getDateTimeType(), dataModel.getDataModel().createLiteral(dataModel.getSf().format(date)));
        dataModel.getDataModel().add(obsInstance, dataModel.getDateType(), dataModel.getDataModel().createTypedLiteral(date.getTime()));
        dataModel.getDataModel().add(obsInstance, dataModel.getIdType(), dataModel.getDataModel().createTypedLiteral(idcount));
        dataModel.getDataModel().add(obsInstance, dataModel.getAltType(), dataModel.getDataModel().createTypedLiteral(altitude));
        dataModel.getDataModel().add(obsInstance, dataModel.getLatType(), dataModel.getDataModel().createTypedLiteral(lat));
        dataModel.getDataModel().add(obsInstance, dataModel.getLonType(), dataModel.getDataModel().createTypedLiteral(lon));
        dataModel.getDataModel().add(obsInstance, dataModel.getVelType(), dataModel.getDataModel().createTypedLiteral(speed));
        dataModel.getDataModel().add(obsInstance, dataModel.getDirType(), dataModel.getDataModel().createTypedLiteral(bearing));
        dataModel.getDataModel().add(obsInstance, dataModel.getSenderType(), dataModel.getDataModel().createTypedLiteral(tm.getDeviceId()));
        dataModel.getDataModel().add(obsInstance, dataModel.getDistanceType(), dataModel.getDataModel().createTypedLiteral(distance));

        //if (mqttproducer != null) mqttproducer.produce(sw.toString());


    }

   /* @Override
    protected void onStop() {

        if (httpserver != null)
            httpserver.stop();
    }
    @Override
    protected void onDestroy() {

        if (httpserver != null)
            httpserver.stop();
    }*/


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.reasoning, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();
        if (id == R.id.action_settings) {
            return true;
        }
        return super.onOptionsItemSelected(item);
    }

    protected boolean isBetterLocation(Location location, Location currentBestLocation) {
        if (currentBestLocation == null) {
            // A new location is always better than no location
            return true;
        }

        // Check whether the new location fix is newer or older
        long timeDelta = location.getTime() - currentBestLocation.getTime();
        boolean isSignificantlyNewer = timeDelta > INTERVAL;
        boolean isSignificantlyOlder = timeDelta < -INTERVAL;
        boolean isNewer = timeDelta > 0;

        // If it's been more than two minutes since the current location, use the new location
        // because the user has likely moved
        if (isSignificantlyNewer) {
            return true;
            // If the new location is more than two minutes older, it must be worse
        } else if (isSignificantlyOlder) {
            return false;
        }

        // Check whether the new location fix is more or less accurate
        int accuracyDelta = (int) (location.getAccuracy() - currentBestLocation.getAccuracy());
        boolean isLessAccurate = accuracyDelta > 0;
        boolean isMoreAccurate = accuracyDelta < 0;
        boolean isSignificantlyLessAccurate = accuracyDelta > 200;

        // Check if the old and new location are from the same provider
        boolean isFromSameProvider = isSameProvider(location.getProvider(),
                currentBestLocation.getProvider());

        // Determine location quality using a combination of timeliness and accuracy
        if (isMoreAccurate) {
            return true;
        } else if (isNewer && !isLessAccurate) {
            return true;
        } else if (isNewer && !isSignificantlyLessAccurate && isFromSameProvider) {
            return true;
        }
        return false;
    }

    private boolean isSameProvider(String provider1, String provider2) {
        if (provider1 == null) {
            return provider2 == null;
        }
        return provider1.equals(provider2);
    }

    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }


    /*private class HTTPServer extends NanoHTTPD {
        public HTTPServer() throws IOException {
            super(8084);

        }
        private GPSObservationModel serverdataModel = new GPSObservationModel();

        @Override public Response serve(IHTTPSession session) {
            Method method = session.getMethod();
            String uri = session.getUri();
            System.out.println(method + " '" + uri + "' ");

            if(serverdataModel == null)
                serverdataModel = new GPSObservationModel();

            Map<String, String> body = new HashMap<String, String>();

            if (Method.PUT.equals(method) || Method.POST.equals(method)) {
                try {
                    session.parseBody(body);
                    String enbody = session.getQueryParameterString();
                    serverdataModel.addENtoDataModel(enbody);

                } catch (IOException ioe) {
                    return new Response(Response.Status.INTERNAL_ERROR, MIME_PLAINTEXT, "SERVER INTERNAL ERROR: IOException: " + ioe.getMessage());
                } catch (ResponseException re) {
                    return new Response(re.getStatus(), MIME_PLAINTEXT, re.getMessage());
                }
            }
            System.out.println(serverdataModel.getDataModel().size());
            if(serverdataModel.getDataModel().size()>200) {
                thread(new Reasoner(serverdataModel), false);
                serverdataModel = new GPSObservationModel();
            }

            return new NanoHTTPD.Response("");


        }
    }*/
    private class CoapServer implements Runnable {




        @Override
        public void run() {
            Server server = new Server();

            server.setExecutor(Executors.newScheduledThreadPool(4));

            server.add(new StoreResource("store"));

            server.start();

            selfTest();
        }

        /*
        * Sends a GET request to itself
        */
        public void selfTest() {
            try {
                Request request = Request.newGet();
                request.setURI("localhost:5683/store");
                request.send();
                Response response = request.waitForResponse(1000);
                System.out.println("received "+response);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private  class StoreResource extends ResourceBase {

            private String content;

            public StoreResource(String name) {
                super(name);
                setObservable(true); // enable observing
                setObserveType(CoAP.Type.CON); // configure the notification type to CONs
                getAttributes().setObservable(); // mark observable in the Link-Format
            }

            @Override
            public void handleGET(CoapExchange exchange) {
                if (content != null) {
                    exchange.respond(content);
                } else {
                    String subtree = LinkFormat.serializeTree(this);
                    exchange.respond(CoAP.ResponseCode.CONTENT, subtree, MediaTypeRegistry.APPLICATION_LINK_FORMAT);
                }
            }

            @Override
            public void handlePOST(CoapExchange exchange) {
                String payload = exchange.getRequestText();
                String[] parts = payload.split("\\?");
                String[] path = parts[0].split("/");
                ch.ethz.inf.vs.californium.server.resources.Resource resource = create(new LinkedList<String>(Arrays.asList(path)));

                Response response = new Response(CoAP.ResponseCode.CREATED);
                response.getOptions().setLocationPath(resource.getURI());
                exchange.respond(response);
                changed();
            }

            @Override
            public void handlePUT(CoapExchange exchange) {
                content = exchange.getRequestText();
                exchange.respond(CoAP.ResponseCode.CHANGED);
                changed();
            }

            @Override
            public void handleDELETE(CoapExchange exchange) {
                this.delete();
                exchange.respond(CoAP.ResponseCode.DELETED);
                changed();
            }

            /**
             * Find the requested child. If the child does not exist yet, create it.
             */
            @Override
            public ch.ethz.inf.vs.californium.server.resources.Resource getChild(String name) {
                ch.ethz.inf.vs.californium.server.resources.Resource resource = super.getChild(name);
                if (resource == null) {
                    resource = new StoreResource(name);
                    add(resource);
                }
                return resource;
            }

            /**
             * Create a resource hierarchy with according to the specified path.
             * @param path the path
             * @return the lowest resource from the hierarchy
             */
            private ch.ethz.inf.vs.californium.server.resources.Resource create(LinkedList<String> path) {
                String segment;
                do {
                    if (path.size() == 0)
                        return this;

                    segment = path.removeFirst();
                } while (segment.isEmpty() || segment.equals("/"));

                StoreResource resource = new StoreResource(segment);
                add(resource);
                return resource.create(path);
            }

        }
    }
    public String streamToString(InputStream fis) {


        StringBuilder inputStringBuilder = new StringBuilder();
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
            String line = bufferedReader.readLine();
            while(line != null){
                inputStringBuilder.append(line);inputStringBuilder.append('\n');
                line = bufferedReader.readLine();
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return inputStringBuilder.toString();

    }



}
