package kstream.demo;

//import nl.amis.streams.model.CustomerAndPolicy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CustomerStreamPipelineHDI {

    static public class CustomerMessage {

        public String address;
        public String customer;
        public Double customertime;
    }

    static public class PolicyMessage {

        public int pvar1;
        public String policyendtime;
        public int policy;
        public String policystarttime;
        public int pvar0;
    }
    static public class CustomerList {


        public ArrayList<CustomerMessage> lst = new ArrayList<>();
        String s = new String();
        public CustomerList() {}
    }

    static public class PolicyList{
        public ArrayList<PolicyMessage> lst = new ArrayList<>();

        public PolicyList() {}

    }

    static public class ClaimMessage {

        public String claimnumber;
        public String claimtime;
        public String claimreporttime;
        public String claimcounter;
    }
    static public class ClaimList{
        public ArrayList<ClaimMessage> lst = new ArrayList<>();
        public ClaimList() {}
    }

    static public class ClaimList2{
        public ArrayList<ClaimList> lst = new ArrayList<>();
        public ClaimList2() {}
    }

    static public class PaymentMessage {

        public Double payment;
        public Double paytime;
        public Integer claimcounter;
        public String claimnumber;
    }
    static public class PaymentList
    {
        public ArrayList<PaymentMessage> lst = new ArrayList<>();
        public PaymentList() {}
    }
  static public class CustomerAndPolicy {

        public CustomerList customerList = new CustomerList();
        public PolicyList policyList = new PolicyList();
        public CustomerAndPolicy() {}

        public CustomerAndPolicy(CustomerList customerList, PolicyList policyList){

            this.customerList = customerList;
            this.policyList = policyList;
        }

    }

    static public class ClaimAndPayment {

        public ClaimList claimList = new ClaimList();
        public PaymentList paymentList = new PaymentList();
        public ClaimAndPayment(ClaimList claimList, PaymentList paymentList){
            this.claimList=claimList;
            this.paymentList=paymentList;
        }
        public ClaimAndPayment(){}

    }
    static public class ClaimAndPayment2{
        public ArrayList<ClaimAndPayment> claimAndPaymentList = new ArrayList<>();
        public ClaimAndPayment2() {}
    }
    static public class CustomerPolicyClaimPayment     {
        public CustomerAndPolicy customerAndPolicy = new CustomerAndPolicy();
        public ClaimAndPayment2 claimAndPayment2 = new ClaimAndPayment2();
        public CustomerPolicyClaimPayment(CustomerAndPolicy customerAndPolicy,ClaimAndPayment2 claimAndPayment2){
            this.customerAndPolicy = customerAndPolicy;
            this.claimAndPayment2 = claimAndPayment2;
        }
        public CustomerPolicyClaimPayment () {}
    }
    private static <T> T waitUntilStoreIsQueryable(final String storeName,
                                                   final QueryableStoreType<T> queryableStoreType,
                                                   final KafkaStreams streams) throws InterruptedException {
        while (true) {
            try {
                return streams.store(storeName, queryableStoreType);
            } catch (InvalidStateStoreException ignored) {
                // store not yet ready for querying
                Thread.sleep(100);
            }
        }
    }

    private static final String APP_ID = "customer-kafka-streaming-demo4";
    private static final String CUSTOMER_TOPIC = "customer";
    private static final String POLICY_TOPIC = "policy";
    private static final String CLAIM_TOPIC = "claim";
    private static final String PAYMENT_TOPIC = "claimpayment"  ;
    private static final String CUSTOMER_OUT_TOPIC = "X7customer_output";
    private static final String CUSTOMER_STORE = "X5CustomerStore";
    private static final String POLICY_STORE = "X5PolicyStore";
    private static final String CLAIM_STORE = "X5ClaimStrStore";
    private static final String PAYMENT_STORE = "X5PaymentStore";
    private static final String CLAIM_AND_PAYMENT_STORE= "X5claimAndPayment2Store";
    private static final String LEVEL = "DEBUG";
    
    public static void main(String[] args) {

        System.out.println("Kafka Streams Customer Demo");
       /************************''SERIALIZERS/DESERIALIZERS*******************/
        // Create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(getProperties());
        final Serde < String > stringSerde = Serdes.String();
        final Serde < Long > longSerde = Serdes.Long();
        final Serde <Integer> integerSerde = Serdes.Integer();

        // define CustomerMessageSerde
        Map < String, Object > serdeProps = new HashMap < String, Object > ();
        final Serializer < CustomerMessage > customerMessageSerializer = new JsonPOJOSerializer < > ();
        serdeProps.put("JsonPOJOClass", CustomerMessage.class);
        customerMessageSerializer.configure(serdeProps, false);

        final Deserializer < CustomerMessage > customerMessageDeserializer = new JsonPOJODeserializer < > ();
        serdeProps.put("JsonPOJOClass", CustomerMessage.class);
        customerMessageDeserializer.configure(serdeProps, false);
        final Serde < CustomerMessage > customerMessageSerde = Serdes.serdeFrom(customerMessageSerializer, customerMessageDeserializer);

        // define customerListSerde
        serdeProps = new HashMap<String, Object>();
        final Serializer<CustomerList> customerListSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", CustomerList.class);
        customerListSerializer.configure(serdeProps, false);

        final Deserializer<CustomerList> customerListDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", CustomerList.class);
        customerListDeserializer.configure(serdeProps, false);
        final Serde<CustomerList> customerListSerde = Serdes.serdeFrom(customerListSerializer, customerListDeserializer );

        // define policySerde
        serdeProps = new HashMap<String, Object>();
        final Serializer<PolicyMessage> policyMessageSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", PolicyMessage.class);
        policyMessageSerializer.configure(serdeProps, false);

        final Deserializer<PolicyMessage> policyMessageDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", PolicyMessage.class);
        policyMessageDeserializer.configure(serdeProps, false);
        final Serde<PolicyMessage> policyMessageSerde = Serdes.serdeFrom(policyMessageSerializer, policyMessageDeserializer );

        // define policyListSerde
        serdeProps = new HashMap<String, Object>();
        final Serializer<PolicyList> policyListSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", PolicyList.class);
        policyListSerializer.configure(serdeProps, false);

        final Deserializer<PolicyList> policyListDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", PolicyList.class);
        policyListDeserializer.configure(serdeProps, false);
        final Serde<PolicyList> policyListSerde = Serdes.serdeFrom(policyListSerializer, policyListDeserializer );

        /*************************************************************************CLAIM*********/

        serdeProps = new HashMap < String, Object > ();
        final Serializer < ClaimMessage > claimMessageSerializer = new JsonPOJOSerializer < > ();
        serdeProps.put("JsonPOJOClass", ClaimMessage.class);
        claimMessageSerializer.configure(serdeProps, false);

        final Deserializer < ClaimMessage > claimMessageDeserializer = new JsonPOJODeserializer < > ();
        serdeProps.put("JsonPOJOClass", ClaimMessage.class);
        claimMessageDeserializer.configure(serdeProps, false);
        final Serde < ClaimMessage > claimMessageSerde = Serdes.serdeFrom(claimMessageSerializer, claimMessageDeserializer);

        // define claimListSerde
        serdeProps = new HashMap<String, Object>();
        final Serializer<ClaimList> claimListSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", ClaimList.class);
        claimListSerializer.configure(serdeProps, false);

        final Deserializer<ClaimList> claimListDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", ClaimList.class);
        claimListDeserializer.configure(serdeProps, false);
        final Serde<ClaimList> claimListSerde = Serdes.serdeFrom(claimListSerializer, claimListDeserializer );

        // define claimList2Serde
        serdeProps = new HashMap<String, Object>();
        final Serializer<ClaimList2> claimList2Serializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", ClaimList2.class);
        claimList2Serializer.configure(serdeProps, false);

        final Deserializer<ClaimList2> claimList2Deserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", ClaimList2.class);
        claimList2Deserializer.configure(serdeProps, false);
        final Serde<ClaimList2> claimList2Serde = Serdes.serdeFrom(claimList2Serializer, claimList2Deserializer );




        /***********************************************PAYMENT********************************************/

        serdeProps = new HashMap < String, Object > ();
        final Serializer < PaymentMessage > paymentMessageSerializer = new JsonPOJOSerializer < > ();
        serdeProps.put("JsonPOJOClass", PaymentMessage.class);
        paymentMessageSerializer.configure(serdeProps, false);

        final Deserializer < PaymentMessage > paymentMessageDeserializer = new JsonPOJODeserializer < > ();
        serdeProps.put("JsonPOJOClass", PaymentMessage.class);
        paymentMessageDeserializer.configure(serdeProps, false);
        final Serde < PaymentMessage > paymentMessageSerde = Serdes.serdeFrom(paymentMessageSerializer, paymentMessageDeserializer);

        // define paymentListSerde
        serdeProps = new HashMap<String, Object>();
        final Serializer<PaymentList> paymentListSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", PaymentList.class);
        paymentListSerializer.configure(serdeProps, false);

        final Deserializer<PaymentList> paymentListDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", PaymentList.class);
        paymentListDeserializer.configure(serdeProps, false);
        final Serde<PaymentList> paymentListSerde = Serdes.serdeFrom(paymentListSerializer, paymentListDeserializer );


        /****************NESTED************************************************************************/
        // define policyAndCustomerSerde
        serdeProps = new HashMap<String, Object>();
        final Serializer<CustomerAndPolicy> customerAndPolicySerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", CustomerAndPolicy.class);
        customerAndPolicySerializer.configure(serdeProps, false);

        final Deserializer<CustomerAndPolicy> customerAndPolicyDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", CustomerAndPolicy.class);
        customerAndPolicyDeserializer.configure(serdeProps, false);
        final Serde<CustomerAndPolicy> customerAndPolicySerde = Serdes.serdeFrom(customerAndPolicySerializer, customerAndPolicyDeserializer );

        //define claimAndPaymentSerde
        serdeProps = new HashMap<String, Object>();
        final Serializer<ClaimAndPayment> claimAndPaymentSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", ClaimAndPayment.class);
        claimAndPaymentSerializer.configure(serdeProps, false);

        final Deserializer<ClaimAndPayment> claimAndPaymentDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", ClaimAndPayment.class);
        claimAndPaymentDeserializer.configure(serdeProps, false);
        final Serde<ClaimAndPayment> claimAndPaymentSerde = Serdes.serdeFrom(claimAndPaymentSerializer, claimAndPaymentDeserializer );

        //define claimAndPayment2Serde
        serdeProps = new HashMap<String, Object>();
        final Serializer<ClaimAndPayment2> claimAndPayment2Serializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", ClaimAndPayment2.class);
        claimAndPayment2Serializer.configure(serdeProps, false);

        final Deserializer<ClaimAndPayment2> claimAndPayment2Deserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", ClaimAndPayment2.class);
        claimAndPayment2Deserializer.configure(serdeProps, false);
        final Serde<ClaimAndPayment2> claimAndPayment2Serde = Serdes.serdeFrom(claimAndPayment2Serializer, claimAndPayment2Deserializer );

        //define customerPolicyClaimPayment
        serdeProps = new HashMap<String, Object>();
        final Serializer<CustomerPolicyClaimPayment> customerPolicyClaimPaymentSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", CustomerPolicyClaimPayment.class);
        customerPolicyClaimPaymentSerializer.configure(serdeProps, false);

        final Deserializer<CustomerPolicyClaimPayment> customerPolicyClaimPaymentDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", CustomerPolicyClaimPayment.class);
        customerPolicyClaimPaymentDeserializer.configure(serdeProps, false);
        final Serde<CustomerPolicyClaimPayment> customerPolicyClaimPaymentSerde = Serdes.serdeFrom(customerPolicyClaimPaymentSerializer, customerPolicyClaimPaymentDeserializer );

        /****************************************************************************************************/
        /*************************KSTREAMS DEFINITIONS******************************************************/

        KStreamBuilder kStreamBuilder = new KStreamBuilder();

       KStream<String, CustomerMessage> customerStream =
               kStreamBuilder.stream(stringSerde, customerMessageSerde, CUSTOMER_TOPIC);


        KStream<Integer, PolicyMessage> policyStream =
                kStreamBuilder.stream(integerSerde, policyMessageSerde, POLICY_TOPIC);

        KStream<Integer, ClaimMessage> claimStream =
                kStreamBuilder.stream(integerSerde, claimMessageSerde, CLAIM_TOPIC);

        KStream<Integer, PaymentMessage> paymentStream =
                kStreamBuilder.stream(integerSerde, paymentMessageSerde, PAYMENT_TOPIC);


        /************************************************************************************************************/

        /**********************************************CUSTOMER******************************************************/



                KTable<Integer,CustomerList> customerGrouped= customerStream

                        .groupBy((key,value) -> Integer.parseInt(value.customer.replaceFirst("cust","")),integerSerde,customerMessageSerde)

                        .aggregate(CustomerList::new,(ckey,custMessage,customerList) -> {
                            customerList.lst.add(custMessage);
                            return customerList;
                        },customerListSerde,CUSTOMER_STORE);


        /**********************************************************************************/

        /******************************************POLICY*********************************/
       KTable<Integer,PolicyList> policyGrouped = policyStream
                .groupBy((k, policy) -> policy.policy,integerSerde,policyMessageSerde)
                .aggregate(
                        PolicyList::new
                        ,
                        (policyKey, policyMsg, policyLst) -> {

                            policyLst.lst.add(policyMsg);

                            return (policyLst);
                        }
                        ,  policyListSerde
                        ,  POLICY_STORE
                );


        /**********************************************************************************/

        /*******************************************CLAIM**********************************/


       KTable<String,ClaimList> claimStrGrouped = claimStream
                .groupBy((k, claim) -> claim.claimnumber,stringSerde,claimMessageSerde)
                .aggregate(
                        ClaimList::new
                        ,
                        (claimKey, claimMsg, claimLst) -> {

                            claimLst.lst.add(claimMsg);
                            // System.out.println("Adding policy "+policyMsg.policy);
                            //System.out.println("Adding policy endtime "+policyMsg.policyendtime);


                            return (claimLst);
                        }
                        , claimListSerde
                        ,  CLAIM_STORE
                );



        /***************************************PAYMENT**********************************/

      KTable<String,PaymentList> paymentGrouped = paymentStream
                .groupBy((k, payment) -> payment.claimnumber,stringSerde,paymentMessageSerde)
                .aggregate(
                        PaymentList::new
                        ,
                        (payKey, payMsg, payLst) -> {



                           payLst.lst.add(payMsg);
                            return (payLst);
                        }
                        , paymentListSerde
                        ,  PAYMENT_STORE
                );



        /**********************************JOIN*******************************************/

       KTable<Integer,CustomerAndPolicy> customerAndPolicyGroupedKTable = customerGrouped.join(policyGrouped,(customer, policy) -> new CustomerAndPolicy(customer,policy));
        //customerAndPolicyKTable.print();
       KTable<String,ClaimAndPayment> claimAndPaymentKTable = claimStrGrouped.leftJoin(paymentGrouped,(claim,payment) -> new ClaimAndPayment(claim,payment));


      KStream<String,ClaimAndPayment> claimAndPaymentKStream = claimAndPaymentKTable.toStream();

       KTable<Integer,ClaimAndPayment2> claimAndPayment2IntGroupedTable =  claimAndPaymentKStream
               .groupBy((k,claimPay) ->
                   (claimPay != null ) ?
                  Integer.parseInt(claimPay.claimList.lst.get(0).claimnumber.split("_")[0]) :  999,integerSerde,claimAndPaymentSerde )
               .aggregate(
                        ClaimAndPayment2::new,
                        (claimKey,claimPay,claimAndPay2) -> {

                            claimAndPay2.claimAndPaymentList.add(claimPay);
                            return claimAndPay2;
                        }
                        ,claimAndPayment2Serde
                        ,CLAIM_AND_PAYMENT_STORE
                );





        KTable<Integer,CustomerPolicyClaimPayment> allJoinedAndCoGrouped = customerAndPolicyGroupedKTable.leftJoin(claimAndPayment2IntGroupedTable,(left,right) -> new CustomerPolicyClaimPayment(left,right));



      //allJoinedAndCoGrouped.through(integerSerde,customerPolicyClaimPaymentSerde,CUSTOMER_OUT_TOPIC);

        String storeName = allJoinedAndCoGrouped.through(integerSerde,customerPolicyClaimPaymentSerde,CUSTOMER_OUT_TOPIC).queryableStoreName();
        System.out.println("Store name "+storeName);





        /********************************************************************************/

        System.out.println("Starting Kafka Streams Customer Demo");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        System.out.println("Now started Customer Demo");

        Timestamp t = new Timestamp(System.currentTimeMillis());
        System.out.println(t);



        Long starttime=System.currentTimeMillis();


        try {
            ReadOnlyKeyValueStore<Integer, CustomerPolicyClaimPayment> keyValueStore = waitUntilStoreIsQueryable(storeName, QueryableStoreTypes.keyValueStore(), kafkaStreams);
            // System.out.println("NumEntries "+keyValueStore.approximateNumEntries());

            Long endtime=System.currentTimeMillis();
            Long duration = (endtime-starttime)/1000;
            Timestamp runtime = new Timestamp(System.currentTimeMillis());

            System.out.println("Running time"+(endtime-starttime)/1000);
            // System.out.println("Endtime"+endtime);
            PrintWriter writer = new PrintWriter("/tmp/print.txt","UTF-8");
            writer.println("Started"+t);
            writer.println("Ended "+runtime);
            System.out.println("Ended"+runtime);
             writer.close();

            CustomerPolicyClaimPayment c = keyValueStore.get(2);
            KeyValueIterator<Integer,CustomerPolicyClaimPayment> kv = keyValueStore.all();
            // KeyValueIterator <Integer, CustomerPolicyClaimPayment> kvrange = keyValueStore.range(1,9999);
            int policies = 0;
            int claims = 0;
            int customers = 0;
            int payments = 0;



            //System.out.println("Num of Policies: "+policies+"Num of customers: "+customers+"Num of claims: "+claims+"Num of payments: "+payments);
            //  System.out.println("Num of Customers grouped by key "+groupedcustomers);
            // System.out.println("Num of Policies grouped by key :"+groupedpolicies);
            // System.out.println("Num of Claims grouped by key: "+groupoedclaims);
            // System.out.println("NumOfPayments grouped by key : "+groupedpayments);


        } catch (Exception e)
        {
            System.out.println(e);
        }



    }

    private static Properties getProperties() {
        Properties settings = new Properties();
        // Set a few key parameters
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        // Kafka bootstrap server (broker to talk to); ubuntu is the host name for my VM running Kafka, port 9092 is where the (single) broker listens
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.101.12.16:9092,10.101.12.6:9092,10.101.12.5:9092,10.101.12.4:9092,10.101.12.9:9092");
        // Apache ZooKeeper instance keeping watch over the Kafka cluster; ubuntu is the host name for my VM running Kafka, port 2181 is where the ZooKeeper listens
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "10.101.12.11:2181,10.101.12.7:2181,10.101.12.12:2181");
        // default serdes for serialzing and deserializing key and value from and to streams in case no specific Serde is specified
        //settings.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        settings.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/customerPipe2");
        // to work around exception Exception in thread "StreamThread-1" java.lang.IllegalArgumentException: Invalid timestamp -1
        // at org.apache.kafka.clients.producer.ProducerRecord.<init>(ProducerRecord.java:60)
        // see: https://groups.google.com/forum/#!topic/confluent-platform/5oT0GRztPBo
        settings.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //settings.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,"metric.reporters");
        settings.put(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG,LEVEL);
        //settings.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG,"metric.reporters");
        settings.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG,LEVEL);
        //settings.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,"metric.reporters");
        settings.put(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG,LEVEL);
        return settings;
    }
}
