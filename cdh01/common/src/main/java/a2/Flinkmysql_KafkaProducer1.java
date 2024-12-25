package a2;

public class Flinkmysql_KafkaProducer1 {
    public static final String Host_name = "cdh03";
    public static final int Port  = 3306;
    public static final String Database = "test";
    public static final String  Table= "aa";
    public static final String User_name = "root";
    public static final String Password= "root";
    public static final String SourceName= "MySQL Source";
//    时间
    public static final Long  Checkpoint= 5000l;

//    kafka的
    public static final String  Servers= "cdh01:9092";
    public static final String  Topic= "topic01";

}
