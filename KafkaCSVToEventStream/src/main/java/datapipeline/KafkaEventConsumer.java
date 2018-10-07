package datapipeline;

import com.espertech.esper.client.*;
import com.espertech.esper.client.annotation.EventRepresentation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaEventConsumer {

    private static int i=0;

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

            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("zookeeper.connect", "localhost:2181");
            props.put("group.id", this.groupId);
            props.put("zookeeper.session.timeout.ms", "500");
            props.put("zookeeper.sync.timeout.ms", "500");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            props.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");

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
            //epService.getEPAdministrator().getConfiguration().addEventType("weatherEvent", result);
            //String[] functionNames = new String[] {FeatureDetectorConstant.FEATUREDETECTED_NAME, FeatureDetectorConstant.FEATUREOUTPUT_NAME};
            //ConfigurationPlugInSingleRowFunction config = new ConfigurationPlugInSingleRowFunction();
            epService.getEPAdministrator().getConfiguration().addPlugInSingleRowFunction("feature_selection", FeatureSelection.class.getName(), "feature_selection");
            String createEventExp = "@EventRepresentation(map) create schema weatherEvent as (prop1 Map)";
            EPStatement statement1 = epService.getEPAdministrator().createEPL(createEventExp);
            ListenerEvent listener = new ListenerEvent();
            statement1.addListener(listener);
            String expression2 ="create context batch10seconds start @now end after 1 sec";
            EPStatement statement2 = epService.getEPAdministrator().createEPL(expression2);
            ListenerEvent listener2 = new ListenerEvent();
            statement2.addListener(listener2);
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

                        String expression = "select distinct feature_selection(first(e), last(e)) from weatherEvent.win:length(3) as e";
                        EPStatement statement = epService.getEPAdministrator().createEPL(expression);
                        statement.addListener(new UpdateListener() {
                            @Override
                            public void update(EventBean[] newEvents, EventBean[] oldEvents) {
                                System.out.println("sum \t" + newEvents[0].getUnderlying() + "\n");
                                System.out.println("old sum \t" + oldEvents[0].getUnderlying() + "\n");
                            }
                        });

                        for (ConsumerRecord<String, JsonNode> record : consumerRecords) {
                            JsonNode jsonNode = record.value();
                            //System.out.println(record.key()+  "," + record.value()+ "," + record.partition()+ "," + record.offset());
                            //eventGenerator(jsonNode);
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

            } catch (WakeupException ex) {
                System.out.println("Exception caught " + ex.getMessage());
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }


        public KafkaConsumer<String, JsonNode> getKafkaConsumer() {
            return this.kafkaConsumer;
        }
    }

}