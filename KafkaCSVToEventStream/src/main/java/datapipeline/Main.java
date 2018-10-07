package datapipeline;


public class Main {
    public static void main(String args[]){

        try{
            KafkaEventProducer producer = new KafkaEventProducer();
            KafkaEventConsumer consumer = new KafkaEventConsumer();

            // Init kafka config
            producer.initKafkaConfig();
            // Init file config - Pass the file name as first parameter
            producer.initFileConfig("sampleData.csv");
            // Send file data to Kafka broker - Pass the topic name as second
            // parameter
            System.out.println("Starting event production");
            producer.sendFileDataToKafka("sample");
            System.out.println("All events sent");

            producer.cleanup();
            System.out.println("All events recieved");

            consumer.createConsumer();
        }catch(Exception e){
            e.printStackTrace();
        }


    }

}

