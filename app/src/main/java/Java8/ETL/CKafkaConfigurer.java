package Java8.ETL;

import java.util.Objects;
import java.util.Properties;

public class CKafkaConfigurer {
    private static Properties properties;

    public static void configureSaslPlain() {
        //如果用-D或者其它方式设置过，这里不再设置。
        if (null == System.getProperty("java.security.auth.login.config")) {
            //请注意将XXX修改为自己的路径。
            System.setProperty("java.security.auth.login.config", Objects.requireNonNull(App.class.getClassLoader().getResource("ckafka_client_jaas.conf")).getPath());
//            System.setProperty("java.security.auth.login.config", getCKafkaProperties().getProperty("java.security.auth.login.config.plain"));
        }
    }

    public synchronized static Properties getCKafkaProperties() {
        if (null != properties) {
            return properties;
        }
        //获取配置文件kafka.properties的内容。
        Properties kafkaProperties = new Properties();
        try {
            kafkaProperties.load(App.class.getClassLoader().getResourceAsStream("kafka.properties"));
        } catch (Exception e) {
            System.out.println("getCKafkaProperties error");
        }
        properties = kafkaProperties;
        return kafkaProperties;
    }
}
