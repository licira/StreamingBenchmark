package ro.tucn.logger;

import org.apache.log4j.Logger;

import java.io.Serializable;

/**
 * Created by Liviu on 4/11/2017.
 */
public class SerializableLogger extends Logger implements Serializable {

    public SerializableLogger(String name) {
        super(name);
    }
}
