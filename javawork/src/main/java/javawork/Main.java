package javawork;

public class Main {

    public static void main(String[] args) {
        System.out.println("Hello World :)");

        /*
        Tutorials in Order:
        - Kafka
        - ParseLog
        - Json
        - Avro
        - KafkaStream
        */
    }

    // USAGE: lstopic 127.0.0.1 9092 ALL|INTERNAL|PUBLIC

    // USAGE: publish 127.0.0.1 9092 topic-name TEXT

    // USAGE: lspart 127.0.0.1 9092 topic-name

    // PhoneBook with Avro :)
    // USAGE add REZA 09126662695  pb.data
    // USAGE add ....
    // USAGe ls pb.data


    String reza = new String("Reza");
    /*
    * -> {
        size: Int
        *array: []char
    }
    */

    // CSV:
    // REZA, 09126662695
    //
    // JSON:
    // "{'name': 'REZA', 'phonenumber': '09126662695'}"
    //
    // AVRO:
    // 1:STR:4:[24, 45, 34, 12] 2:DEC:[43878,4389843,43]
    //


}
