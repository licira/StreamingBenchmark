package ro.tucn.generator.helper.entity;

import ro.tucn.kMeans.Point;

/**
 * Created by Liviu on 6/27/2017.
 */
public class KMeansJSONHelper extends JSONHelper {

    public String getMessageKey(Point point) {
        return Long.toString(point.getId());
    }

    public String getMessageValue(Point point) {
        double[] coordinates = point.getCoordinates();
        int locationSize = coordinates.length;
        StringBuilder messageData = new StringBuilder();
        int i;
        for (i = 0; i < locationSize - 1; i++) {
            messageData.append(coordinates[i]);
            messageData.append(" ");
        }
        messageData.append(coordinates[i]);
        return toJson(point);
    }
}
