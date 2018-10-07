package datapipeline;


import com.espertech.esper.client.EventBean;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class FeatureSelection {

    public static Map feature_selection(Map firstEvent, Map lastEvent) {

        final Map<String, Object> result = new HashMap<String, Object>();

        firstEvent.values().removeIf(Objects::isNull);

        lastEvent.values().removeIf(Objects::isNull);

        System.out.println("first" + firstEvent);

        System.out.println("last" + lastEvent);

        try{
            for (Object k : lastEvent.keySet())
            {
                //System.out.println("keys" + k.toString() + firstEvent.get(k));
                Double first = new Double(firstEvent.get(k).toString());
                Double last = new Double(lastEvent.get(k).toString());
                //System.out.println("calc" + value);
                if((last - first)>0 || (last - first)<0){
                    result.put(k.toString(), firstEvent.get(k));
                    result.put(k.toString(), lastEvent.get(k));
                }
            }
        } catch (NullPointerException np) {
            np.printStackTrace();
        }
        return result;
    }

}
