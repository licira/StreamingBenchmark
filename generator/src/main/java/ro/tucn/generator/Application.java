package ro.tucn.generator;

/**
 * Created by Liviu on 4/5/2017.
 */
public class Application {

    public static void main(String args[]) throws InterruptedException {
        if(args.length > 0) {
            StreamGenerator streamGenerator = new StreamGenerator();
            streamGenerator.run(args);
        }
    }
}
