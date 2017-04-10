package ro.tucn.frame.functions;

/**
 * Created by Liviu on 4/8/2017.
 */
public interface AssignTimeFunction<T>  extends  SerializableFunction {

    long assign(T var1);
}
