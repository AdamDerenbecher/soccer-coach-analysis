package kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CoachProducer {

    private final Logger logger = LoggerFactory.getLogger(CoachProducer.class.getName());

    public static void main(String[] args) {
        new CoachProducer().run();
    }

    private void run() {
        logger.info("logger is working!");
    }
}
