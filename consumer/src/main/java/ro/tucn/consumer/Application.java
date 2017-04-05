package ro.tucn.consumer;

/**
 * Created by Liviu on 4/5/2017.
 */
public class Application {

    public static void main(String[] args) {
        Consumer consumer = new Consumer(args);
        consumer.run();
    }
}