package kafka;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

class CoachProducer {

    //Instantiate a logger object to be used for debugging later
    private final Logger logger = LoggerFactory.getLogger(CoachProducer.class.getName());

    //Create list of strings that will be used to hold the names of the coaches we want to to sentiment analysis on
    private final List<String> coachNames = Lists.newArrayList("Bob Bradley", "Greg Berhalter");

    //Main method that just calls the run method
    public static void main(String[] args) {

        String consumerKey = null;
        String consumerSecret = null;
        String token = null;
        String secret = null;
        String bootstrapServer = null;

        try (InputStream input = CoachProducer.class.getClassLoader().getResourceAsStream("my.properties")) {

            Properties prop = new Properties();

            if (input == null) {
                System.out.println("Sorry, unable to find my.properties");
                return;
            }

            //load a properties file from class path, inside static method
            prop.load(input);

            //Create variables for authenticating
            //Grabbing the actual values from a properties file
            consumerKey = prop.getProperty("consumer-key");
            consumerSecret = prop.getProperty("consumer-secret");
            token = prop.getProperty("access-token");
            secret = prop.getProperty("access-token-secret");
            bootstrapServer = prop.getProperty("bootstrap-server");


        } catch (IOException ex) {
            ex.printStackTrace();
        }


        new CoachProducer().run(consumerKey, consumerSecret, token, secret, bootstrapServer);
    }

    //Run method the does all of the work
    private void run(String consumerKey, String consumerSecret, String token, String secret, String bootstrapServer) {
        logger.info("logger is working");

        /*
        BlockingQueue = A Queue that additionally supports operations that wait for the queue to become non-empty
                        when retrieving an element, and wait for space to become available in the queue when storing
                        an element.
        LinkedBlockingQueue = This queue orders elements FIFO (first-in-first-out). The head of the queue is that
                              element that has been on the queue the longest time. The tail of the queue is that
                              element that has been on the queue the shortest time. New elements are inserted at the
                              tail of the queue, and the queue retrieval operations obtain elements at the head of
                              the queue.
        */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        logger.info("Calling createTwitterClient method");

        //Creating our Twitter client object
        Client client = createTwitterClient(msgQueue, consumerKey, consumerSecret, token, secret);

        logger.info("Twitter client has been created");

        logger.info("Calling createKafkaProducer method");

        //Create the kafka producer object
        KafkaProducer<String, String> producer = createKafkaProducer(bootstrapServer);

        logger.info("Kafka Producer has been created");
    }

    //Method that is used to create the twitter client in the run method
    private Client createTwitterClient(BlockingQueue<String> msgQueue, String consumerKey,
                                       String consumerSecret, String token, String secret) {

        logger.info("Creating twitter client");

        //Set the host and end point to have complete url
        Hosts coachHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint coachEndpoint = new StatusesFilterEndpoint();

        logger.info("Host Name is : " + coachHosts.nextHost());
        logger.info("End Point is : " + coachEndpoint.getPath());

        //Set the terms we want to be looking for.  In our case it is coach's names
        coachEndpoint.trackTerms(coachNames);

        logger.info("Terms being searched for on Twitter : " + coachNames.toString());

        //Creating an authentication object so we can access the twitter api
        Authentication twitterAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        //Creating the object that we can reference later in the run method so that we can push the twitter
        //data into Kafka
        ClientBuilder builder = new ClientBuilder()
                .name("Coach-Client_01")
                .hosts(coachHosts)
                .authentication(twitterAuth)
                .endpoint(coachEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        //Finally, return the object
        return builder.build();

    }

    private KafkaProducer<String, String> createKafkaProducer(String bootstrapServer) {

        //Create a properties object
        Properties properties = new Properties();

        //Setting the ip for the bootstrap server from the properties file
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        //Setting the serializer for the key and value of the incoming twitter payloads
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Setting the producer to be Idempotent (a.k.a. no duplicates if things go wrong and we have to send
        //payloads again)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        //In order to be Idempotent we have to acknowledge whether the message was received or not
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        //Setting the retries on failure the maximum that it can be (2,147,483,647)
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        //This is a limitation set by Kafka.  If Idempotent is true then 5 is the max here
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //Setting the compression type.  Can be one of the following :
        //  gzip, snappy, lz4, zstd
        //Going with snappy as it is more geared towards performance than maximizing compression
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        //Setting "linger" time to 20 milliseconds or .02 seconds
        //This is basically saying that if we don't reach the batch size (next property below), then we will wait
        //for this amount of time before sending the batch of messages into Kafka
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");

        //Setting the max batch size for a message into Kafka at 32 KB
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        //Create the actual producer object with the properties set above

        //Finally,  return the producer object
        return new KafkaProducer<>(properties);
    }


}
