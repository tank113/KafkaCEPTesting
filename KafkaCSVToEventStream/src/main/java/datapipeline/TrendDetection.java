package datapipeline;


import com.espertech.esper.client.EventBean;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TrendDetection {

    public static Map detect_trend(Map firstEvent, Map prevEvent, Map lastEvent) {



        Map<String, Object> resultInc = new HashMap<String, Object>();

        Map<String, Object> resultDec = new HashMap<String, Object>();

        Map<String, Object> resultTurn = new HashMap<String, Object>();

        Map<String, Object> trend = new HashMap<String, Object>();

        //System.out.println("prevprev" + prevprevEvent);

        Map first = objectToMap(firstEvent);
        Map second = objectToMap(prevEvent);
        Map third = objectToMap(lastEvent);


        System.out.println("New" + first);

        System.out.println("Middle" + second);

        System.out.println("last" + third);


        for (Object k : first.keySet()){
            Double firstVal = new Double(first.get(k).toString());
            Double secVal = new Double(second.get(k).toString());
            Double lastVal = new Double(third.get(k).toString());
            //System.out.println("calc" + value);

            // It will be decreasing because first is the recent event and third is the last event in the window (See example in notes)
            if((lastVal>secVal) && (secVal>firstVal)){
                resultInc.put(k.toString(), third.get(k));
                trend.put("Decreasing", resultInc);
            }
            else if((lastVal<secVal) && (secVal<firstVal)){
                resultDec.put(k.toString(), third.get(k));
                trend.put("Increasing", resultDec);
            }
            else{
                resultTurn.put(k.toString(), second.get(k));
                trend.put("Turn", resultTurn);
            }

        }

       /* for (int i=0; i<fullEvent.length; i++) {

            Map firstEvent = objectToMap(fullEvent[i]);
            Map secondEvent = objectToMap(fullEvent[i+1]);

            try {
                for (Object k : secondEvent.keySet()) {
                    //System.out.println("keys" + k.toString() + firstEvent.get(k));
                    Double first = new Double(firstEvent.get(k).toString());
                    Double second = new Double(secondEvent.get(k).toString());

                    int count = 0;

                    count = sum + count;


                    if (second > first){
                        count = count + 1;
                    }

                    resultCount.put(k.toString(), count);


                }


            } catch (NullPointerException np) {
                np.printStackTrace();
            }


            //System.out.println("full" + resultCount);

            sum = sum + 1;


        }

        for (int j=0; j<fullEvent.length; j++){

            Map firstEvent = objectToMap(fullEvent[j]);

            try {
                for (Object k : firstEvent.keySet()) {
                    //System.out.println("keys" + k.toString() + firstEvent.get(k));
                    int count_value = Integer.parseInt(resultCount.get(k).toString());
                    if (count_value == 3){
                        result.put(k.toString(), firstEvent.get(k));

                    }
                }


            } catch (NullPointerException np) {
                np.printStackTrace();
            }
        }*/


        return trend;
    }

    public static Map objectToMap(Map<String, Object> result_output) {
        Map<String, Object> output = new HashMap<>();
        try{
            Map.Entry<String, Object> entry = result_output.entrySet().iterator().next();
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.convertValue(entry.getValue(), JsonNode.class);
            output = mapper.readValue(node.toString(),
                    new TypeReference<HashMap<String, Object>>() {
                    });
        }catch (IOException e){

        }

        return output;


    }


}
