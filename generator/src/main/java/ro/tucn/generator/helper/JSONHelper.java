package ro.tucn.generator.helper;

import com.google.gson.Gson;

import java.util.List;

/**
 * Created by Liviu on 6/27/2017.
 */
public abstract class JSONHelper {

    private Gson gson;

    public JSONHelper() {
        gson = new Gson();
    }

    public String toJson(Object o) {
        return gson.toJson(o);
    }

    public String[] getJsons(List<Object> objects) {
        String[] jsons = new String[objects.size()];
        int i = 0;
        for (Object o : objects) {
            String json = toJson(o);
            jsons[i++] = json;
        }
        return jsons;
    }
}
