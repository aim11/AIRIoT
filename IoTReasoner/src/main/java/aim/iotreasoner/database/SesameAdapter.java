package aim.iotreasoner.database;

import aim.iotreasoner.querying.QueryBuilder;
import aim.iotreasoner.IoTReasoner;
import com.hp.hpl.jena.rdf.model.Statement;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.*;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.http.HTTPRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.UUID;

/**
 * Created by amaarala on 12/11/13.
 */
public class SesameAdapter {

    private static String obsURI = "http://localhost/Schema/ontology#";

    //private static SesameAdapter adapter;
    private RepositoryConnection con = null;
    private String sesameServer = "http://cse-cn0004.oulu.fi:10010/openrdf-sesame";
    private UUID uuid;

    public SesameAdapter(String URI, RDFFormat sformat){
        this.dataFormat = sformat;
        obsURI = URI;

    }
    private static final Logger LOGGER = LoggerFactory
            .getLogger(IoTReasoner.class);

    public SesameAdapter(String URI) {
        obsURI = URI;
    }

    public void setDataFormat(RDFFormat dataFormat) {
        this.dataFormat = dataFormat;
    }

    RDFFormat dataFormat = RDFFormat.RDFXML;


    public void addStatement(RepositoryConnection connection, Statement statement){
        try {

            //connection.begin();


            // Add the first file
            ValueFactory factory =  connection.getRepository().getValueFactory();
            URI uri1 = factory.createURI(statement.getResource().getURI().toString());
            URI uri2 = factory.createURI(statement.getPredicate().getURI().toString());
            URL urL = null;

            try {
                urL = new URL("http://localhost/Schema/ontology#");
            } catch (MalformedURLException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }

            URI ontURI = factory.createURI(obsURI + "/ontology#");

            //RepositoryResult<org.openrdf.model.Statement> stmts = connection.getStatements(null, null, null, true);
            org.openrdf.model.Statement stm = factory.createStatement(ontURI, org.openrdf.model.vocabulary.RDF.TYPE, uri1);
            connection.add(stm);

            connection.commit();
        } catch (RepositoryException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.

        }finally {
            // Whatever happens, we want to close the connection when we are done.
            try {
                connection.close();
            } catch (RepositoryException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }

    public void addStatements(RepositoryConnection connection, StringWriter writer){
        //long time = (new Date()).getTime();
        try {
            //System.out.println("------------------ ADDING STATEMENTS -------------------- ");
            //connection.begin();

            // Add the first file
            ValueFactory factory =  connection.getRepository().getValueFactory();

            StringReader reader = new StringReader(writer.toString());

            connection.add(reader, obsURI, dataFormat);

            connection.commit();

        } catch (RepositoryException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.

        } catch (RDFParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //long c = ((new Date()).getTime())-time;
            // Whatever happens, we want to close the connection when we are done.
            try {
                connection.close();
                //LOGGER.info("sa " + (new Date()).getTime());
            } catch (RepositoryException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

    }

    public void startGraphQuery(RepositoryConnection connection, StringWriter data){
        uuid = UUID.randomUUID();
        this.addInferNDelete(connection, data, "");
        /*try {
            //System.out.println("------------------ ADDING STATEMENTS -------------------- ");
            //connection.begin();

            Update updateQ = connection.prepareUpdate(QueryLanguage.SPARQL, "CREATE GRAPH <http://localhost/Schema/"+uuid.toString()+">");
            updateQ.execute();

        } catch (RepositoryException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.

        }catch (MalformedQueryException e) {
            e.printStackTrace();
        } catch (UpdateExecutionException e) {
            e.printStackTrace();
        } finally {
            //long c = ((new Date()).getTime())-time;
            // Whatever happens, we want to close the connection when we are done.

            this.addInferNDelete(this.getSesameConnection(), data, "");

            try {
                connection.close();
            } catch (RepositoryException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }*/
    }

    public void addInferNDelete(RepositoryConnection connection, StringWriter writer, String deleteQuery){
        //long time = (new Date()).getTime();

        try {
            //System.out.println("------------------ ADDING STATEMENTS -------------------- ");
            //connection.begin();


            // Add the first file
            ValueFactory factory =  connection.getRepository().getValueFactory();

            StringReader reader = new StringReader(writer.toString());


            URI context = factory.createURI("http://localhost/Schema/"+uuid.toString());
            connection.add(reader, "http://localhost/Schema/"+uuid.toString(), dataFormat, (Resource) context);

            connection.commit();

        } catch (RepositoryException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.

        } catch (RDFParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            //long c = ((new Date()).getTime())-time;
            // Whatever happens, we want to close the connection when we are done.

            this.updateNDeleteQuery(connection, QueryBuilder.inferJam2("http://localhost/Schema/" + uuid.toString()));
        }
    }



    public void updateNDeleteQuery(RepositoryConnection connection, String query){
        //long time = (new Date()).getTime();
        try {
            //System.out.println("------------------ ADDING STATEMENTS -------------------- ");
            //connection.begin();

            Update jamQ = connection.prepareUpdate(QueryLanguage.SPARQL, query);
            jamQ.execute();
            connection.commit();
            Update longstopQ = connection.prepareUpdate(QueryLanguage.SPARQL, QueryBuilder.inferLongStop2("http://localhost/Schema/" + uuid.toString()));
            longstopQ.execute();
            connection.commit();
            Update highavgQ = connection.prepareUpdate(QueryLanguage.SPARQL, QueryBuilder.inferHighAvgSpeed2("http://localhost/Schema/" + uuid.toString()));
            highavgQ.execute();
            connection.commit();

        } catch (RepositoryException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.

        }catch (MalformedQueryException e) {
            e.printStackTrace();
        } catch (UpdateExecutionException e) {
            e.printStackTrace();
        } finally {
            //long c = ((new Date()).getTime())-time;
            // Whatever happens, we want to close the connection when we are done.
            //TODO: do not drop, include in infer qury?

            //LOGGER.info("sa " + (new Date()).getTim
            this.updateQuery(connection, "DROP GRAPH <http://localhost/Schema/"+uuid.toString()+">");
        }
    }

    private void graphQuery(RepositoryConnection connection, String query) {

        try {
            //System.out.println("------------------ ADDING STATEMENTS -------------------- ");
            //connection.begin();

            GraphQuery graphQ = connection.prepareGraphQuery(QueryLanguage.SPARQL, query);

            graphQ.evaluate();

        } catch (RepositoryException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.

        }catch (MalformedQueryException e) {
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            e.printStackTrace();
        } finally {
            //long c = ((new Date()).getTime())-time;
            // Whatever happens, we want to close the connection when we are done.

            LOGGER.info("sa " + (new Date()).getTime());
            try {
                connection.close();
            } catch (RepositoryException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }


    public void updateQuery(RepositoryConnection connection, String query){
        //long time = (new Date()).getTime();
        try {
            //System.out.println("------------------ ADDING STATEMENTS -------------------- ");
            //connection.begin();

            Update updateQ = connection.prepareUpdate(QueryLanguage.SPARQL, query);
            updateQ.execute();
            connection.commit();
        } catch (RepositoryException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.

        }catch (MalformedQueryException e) {
            e.printStackTrace();
        } catch (UpdateExecutionException e) {
            e.printStackTrace();
        } finally {
            //long c = ((new Date()).getTime())-time;
            // Whatever happens, we want to close the connection when we are done.

            LOGGER.info("sa " + (new Date()).getTime());
            try {
                connection.close();
            } catch (RepositoryException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }

    public RepositoryConnection getSesameConnection(){

        if(con == null){
            //File dataDir = new File("./");
            //MemoryStore memStore = new MemoryStore(dataDir);
            //memStore.setSyncDelay(1000L);
            //Repository repository = new SailRepository(memStore);

            String repositoryID = "iot";

            Repository repository = new HTTPRepository(sesameServer, repositoryID);
            try {

                repository.initialize();

                con = repository.getConnection();

            } catch (RepositoryException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
        return con;


    }

    public RepositoryConnection createConnection(){



            //File dataDir = new File("./");
            //MemoryStore memStore = new MemoryStore(dataDir);
            //memStore.setSyncDelay(1000L);
            //Repository repository = new SailRepository(memStore);
        RepositoryConnection connection = null;
            String repositoryID = "iot";

            Repository repository = new HTTPRepository(sesameServer, repositoryID);
            try {

                repository.initialize();

                connection = repository.getConnection();

            } catch (RepositoryException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }

        return connection;


    }




    public static void setObsURI(String obsURI) {
        SesameAdapter.obsURI = obsURI;
    }

}
