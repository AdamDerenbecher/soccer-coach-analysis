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

        } catch (IOException ex) {
            ex.printStackTrace();
        }


        new CoachProducer().run(consumerKey, consumerSecret, token, secret);
    }

    //Run method the does all of the work
    private void run(String consumerKey, String consumerSecret, String token, String secret) {
        logger.info("logger is working!");

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

        logger.info("Calling createTwitterClient method.");

        //Creating our Twitter client object
        Client client = createTwitterClient(msgQueue, consumerKey, consumerSecret, token, secret);

        logger.info("Twitter client has been created!");
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


}
