package datapipeline;

import com.espertech.esper.client.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dataanalysis.FeatureSelection;
import dataanalysis.TrendDetection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaEventConsumer {

    private static int i=0;
    private static int count=0;

    /**
     * Initialize Kafka configuration
     */
    public void createConsumer() throws InterruptedException {

        ConsumerThread consumerRunnable = new ConsumerThread("sample","sample-consumer-group");
        consumerRunnable.start();

        //consumerRunnable.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerRunnable.join();
    }

    private static class ConsumerThread extends Thread {

        private String topicName;
        private String groupId;
        private KafkaConsumer<String, JsonNode> kafkaConsumer;



        public ConsumerThread(String topicName, String groupId) {
            this.topicName = topicName;
            this.groupId = groupId;
        }

        public void run() {
            ConsumerProperties prop = new ConsumerProperties();
            Properties props = prop.propertiesKafkaConsumer(this.groupId);
            kafkaConsumer = new KafkaConsumer<>(props);
            // Subscribe to the topic.
            kafkaConsumer.subscribe(Collections.singletonList(topicName));
            System.out.println("consumer" + kafkaConsumer);
            //ObjectMapper mapper = new ObjectMapper();
            final int giveUp = 100;
            int noRecordsCount = 0;
            Map<String, Object> result = new HashMap<>();

            //Start processing messages
            Configuration config = new Configuration();
            EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(config);

            // Define Single-row function in ESPER
            epService.getEPAdministrator().getConfiguration().addPlugInSingleRowFunction("feature_selection", FeatureSelection.class.getName(), "feature_selection");
            epService.getEPAdministrator().getConfiguration().addPlugInSingleRowFunction("detect_trend", TrendDetection.class.getName(), "detect_trend");

            // Create WeatherEvent using Map properties
            String createEventExp = "@EventRepresentation(map) create schema weatherEvent as (prop1 Map)";
            EPStatement statement1 = epService.getEPAdministrator().createEPL(createEventExp);
            ListenerEvent listener = new ListenerEvent();
            statement1.addListener(listener);

            // Create WeatherTrendEvent using Map properties
            String createEventExpTrends = "@EventRepresentation(map) create schema weatherTrendEvent as (prop1 Map)";
            EPStatement statementTrend = epService.getEPAdministrator().createEPL(createEventExpTrends);
            ListenerEvent listenerForTrend = new ListenerEvent();
            statementTrend.addListener(listenerForTrend);

            // Create Batch context of 1 seconds
            String expression2 ="create context batch10seconds start @now end after 1 sec";
            EPStatement statement2 = epService.getEPAdministrator().createEPL(expression2);
            ListenerEvent listener2 = new ListenerEvent();
            statement2.addListener(listener2);

            KafkaEventProducer kProd = new KafkaEventProducer();

            try {
                while (true) {
                    ConsumerRecords<String, JsonNode> consumerRecords = kafkaConsumer.poll(100);
                    if (consumerRecords.count() == 0) {
                        noRecordsCount++;
                        if (noRecordsCount > giveUp) break;
                        else continue;
                    }
                    if (consumerRecords.isEmpty()) {
                        System.out.println("empty");
                    } else {


                            EPRuntime runtime = epService.getEPRuntime();

                            // Esper Query for Feature Selection using single row function which returns the map with features
                            String expression = "select distinct feature_selection(first(e), last(e)) from weatherEvent.win:length(3) as e";
                            EPStatement statement = epService.getEPAdministrator().createEPL(expression);
                            statement.addListener(new UpdateListener() {

                                @Override
                                public void update(EventBean[] newEvents, EventBean[] oldEvents) {
                                    try {

                                        Map<String, Object> result_output = new HashMap<>();
                                        ObjectMapper mapper = new ObjectMapper();
                                        JsonNode node = mapper.convertValue(newEvents[0].getUnderlying(), JsonNode.class);
                                        result_output = mapper.readValue(node.toString(),
                                                new TypeReference<HashMap<String, Object>>() {
                                                });
                                        Map.Entry<String,Object> entry = result_output.entrySet().iterator().next();
                                        kProd.initKafkaConfig();
                                        kProd.sendOutputToKafka(entry.getValue(), "output");
                                        runtime.sendEvent(result_output, "weatherTrendEvent");

                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                    System.out.println("old sum \t" + oldEvents[0].getUnderlying() + "\n");

                                }



                            });

                            // Esper Query for Trends detection using single row function which returns the nested map with increasing, decreasing and Turn trends features.
                            String expressionTrend = "select detect_trend(trendEvent, prev(trendEvent), first(trendEvent)) from weatherTrendEvent.win:length(3) as trendEvent";
                            EPStatement statementForTrend = epService.getEPAdministrator().createEPL(expressionTrend);
                            statementForTrend.addListener (new UpdateListener() {
                                @Override
                                public void update(EventBean[] newEvents, EventBean[] oldEvents) {
                                    System.out.println("event \t" + newEvents[0].getUnderlying() + "\n");
                                    System.out.println("old event \t" + oldEvents[0].getUnderlying() + "\n");

                            }

                        });


                            for (ConsumerRecord<String, JsonNode> record : consumerRecords) {
                                JsonNode jsonNode = record.value();
                                ObjectMapper mapper = new ObjectMapper();

                                result = mapper.readValue(jsonNode.toString(),
                                        new TypeReference<HashMap<String, Object>>() {
                                        });
                                System.out.println("result" + result);
                                runtime.sendEvent(result, "weatherEvent");

                            }


                            kafkaConsumer.commitAsync();

                    }


                }
            }catch (WakeupException ex) {
                System.out.println("Exception caught " + ex.getMessage());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }



        public KafkaConsumer<String, JsonNode> getKafkaConsumer() {
            return this.kafkaConsumer;
        }
    }

}
