//package uk.ac.ed.acp.cw2.data;
//
//import lombok.Getter;
//import lombok.Setter;
//
///**
// * Represents configuration settings for runtime environments, which are populated using environment variables.
// * This class includes settings related to Redis, RabbitMQ, and Kafka. It provides static constants for
// * environment variable names and methods to retrieve and validate the required environment settings.
// */
//@Getter
//@Setter
//public class RuntimeEnvironment {
//
//    public static final String REDIS_HOST_ENV_VAR = "REDIS_HOST";
//    public static final String REDIS_PORT_ENV_VAR = "REDIS_PORT";
//    public static final String RABBITMQ_HOST_ENV_VAR = "RABBITMQ_HOST";
//    public static final String RABBITMQ_PORT_ENV_VAR = "RABBITMQ_PORT";
//    public static final String KAFKA_BOOTSTRAP_SERVERS_ENV_VAR = "KAFKA_BOOTSTRAP_SERVERS";
//
//    public static final String KAFKA_SECURITY_PROTOCOL_ENV_VAR = "KAFKA_SECURITY_PROTOCOL";
//    public static final String KAFKA_SASL_MECHANISM_ENV_VAR = "KAFKA_SASL_MECHANISM";
//    public static final String KAFKA_SASL_JAAS_CONFIG_ENV_VAR = "KAFKA_SASL_JAAS_CONFIG";
//
//    private String redisHost;
//    private int redisPort;
//    private String rabbitmqHost;
//    private int rabbitmqPort;
//    private String kafkaBootstrapServers;
//    private String kafkaSecurityProtocol;
//    private String kafkaSaslMechanism;
//    private String kafkaSaslJaasConfig;
//
//    /**
//     * Configures and retrieves the runtime environment settings by reading from
//     * predefined environment variables. If specific environment variables are not
//     * set, it uses default values for the configuration. Validates necessary variables
//     * required for Kafka security setup if security is enabled.
//     *
//     * @return a configured {@code RuntimeEnvironment} object containing runtime settings
//     *         such as Kafka, Redis, and RabbitMQ configurations.
//     * @throws RuntimeException if Kafka security is enabled but the required security
//     *         configuration variables are missing.
//     */
//    public static RuntimeEnvironment getEnvironment() {
//        RuntimeEnvironment settings = new RuntimeEnvironment();
//
//        settings.setKafkaBootstrapServers(System.getenv(KAFKA_BOOTSTRAP_SERVERS_ENV_VAR) == null ? "localhost:9092" : System.getenv(KAFKA_BOOTSTRAP_SERVERS_ENV_VAR));
//        settings.setRedisHost(System.getenv(REDIS_HOST_ENV_VAR) == null ? "localhost" : System.getenv(REDIS_HOST_ENV_VAR));
//        settings.setRedisPort(System.getenv(REDIS_PORT_ENV_VAR) == null ? 6379 : Integer.parseInt(System.getenv(REDIS_PORT_ENV_VAR)));
//        settings.setRabbitmqHost(System.getenv(RABBITMQ_HOST_ENV_VAR) == null ? "localhost" : System.getenv(RABBITMQ_HOST_ENV_VAR));
//        settings.setRabbitmqPort(System.getenv(RABBITMQ_PORT_ENV_VAR) == null ? 5672 : Integer.parseInt(System.getenv(RABBITMQ_PORT_ENV_VAR)));
//
//        // if the security is enabled then all must be set - otherwise no security is happening
//        if (System.getenv(KAFKA_SECURITY_PROTOCOL_ENV_VAR) != null) {
//            if (System.getenv(KAFKA_SASL_MECHANISM_ENV_VAR) == null || System.getenv(KAFKA_SASL_JAAS_CONFIG_ENV_VAR) == null || System.getenv(KAFKA_SECURITY_PROTOCOL_ENV_VAR) == null) {
//                throw new RuntimeException("if security is set up all 3 security attributes must be specified");
//            }
//
//            settings.setKafkaSecurityProtocol(System.getenv(KAFKA_SECURITY_PROTOCOL_ENV_VAR));
//            settings.setKafkaSaslMechanism(System.getenv(KAFKA_SASL_MECHANISM_ENV_VAR));
//            settings.setKafkaSaslJaasConfig(System.getenv(KAFKA_SASL_JAAS_CONFIG_ENV_VAR));
//        }
//
//        return settings;
//    }
//}

package uk.ac.ed.acp.cw2.data;

/**
 * Represents configuration settings for runtime environments, which are populated using environment variables.
 * This class includes settings related to Redis, RabbitMQ, and Kafka. It provides static constants for
 * environment variable names and methods to retrieve and validate the required environment settings.
 */
public class RuntimeEnvironment {

    public static final String REDIS_HOST_ENV_VAR = "REDIS_HOST";
    public static final String REDIS_PORT_ENV_VAR = "REDIS_PORT";
    public static final String RABBITMQ_HOST_ENV_VAR = "RABBITMQ_HOST";
    public static final String RABBITMQ_PORT_ENV_VAR = "RABBITMQ_PORT";
    public static final String KAFKA_BOOTSTRAP_SERVERS_ENV_VAR = "KAFKA_BOOTSTRAP_SERVERS";

    public static final String KAFKA_SECURITY_PROTOCOL_ENV_VAR = "KAFKA_SECURITY_PROTOCOL";
    public static final String KAFKA_SASL_MECHANISM_ENV_VAR = "KAFKA_SASL_MECHANISM";
    public static final String KAFKA_SASL_JAAS_CONFIG_ENV_VAR = "KAFKA_SASL_JAAS_CONFIG";

    private String redisHost;
    private int redisPort;
    private String rabbitmqHost;
    private int rabbitmqPort;
    private String kafkaBootstrapServers;
    private String kafkaSecurityProtocol;
    private String kafkaSaslMechanism;
    private String kafkaSaslJaasConfig;

    // Getters and Setters
    public String getRedisHost() {
        return redisHost;
    }

    public void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    public String getRabbitmqHost() {
        return rabbitmqHost;
    }

    public void setRabbitmqHost(String rabbitmqHost) {
        this.rabbitmqHost = rabbitmqHost;
    }

    public int getRabbitmqPort() {
        return rabbitmqPort;
    }

    public void setRabbitmqPort(int rabbitmqPort) {
        this.rabbitmqPort = rabbitmqPort;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public String getKafkaSecurityProtocol() {
        return kafkaSecurityProtocol;
    }

    public void setKafkaSecurityProtocol(String kafkaSecurityProtocol) {
        this.kafkaSecurityProtocol = kafkaSecurityProtocol;
    }

    public String getKafkaSaslMechanism() {
        return kafkaSaslMechanism;
    }

    public void setKafkaSaslMechanism(String kafkaSaslMechanism) {
        this.kafkaSaslMechanism = kafkaSaslMechanism;
    }

    public String getKafkaSaslJaasConfig() {
        return kafkaSaslJaasConfig;
    }

    public void setKafkaSaslJaasConfig(String kafkaSaslJaasConfig) {
        this.kafkaSaslJaasConfig = kafkaSaslJaasConfig;
    }

    /**
     * Configures and retrieves the runtime environment settings by reading from
     * predefined environment variables. If specific environment variables are not
     * set, it uses default values for the configuration. Validates necessary variables
     * required for Kafka security setup if security is enabled.
     *
     * @return a configured {@code RuntimeEnvironment} object containing runtime settings
     *         such as Kafka, Redis, and RabbitMQ configurations.
     * @throws RuntimeException if Kafka security is enabled but the required security
     *         configuration variables are missing.
     */
    public static RuntimeEnvironment getEnvironment() {
        RuntimeEnvironment settings = new RuntimeEnvironment();

        settings.setKafkaBootstrapServers(System.getenv(KAFKA_BOOTSTRAP_SERVERS_ENV_VAR) == null ? "localhost:9092" : System.getenv(KAFKA_BOOTSTRAP_SERVERS_ENV_VAR));
        settings.setRedisHost(System.getenv(REDIS_HOST_ENV_VAR) == null ? "localhost" : System.getenv(REDIS_HOST_ENV_VAR));
        settings.setRedisPort(System.getenv(REDIS_PORT_ENV_VAR) == null ? 6379 : Integer.parseInt(System.getenv(REDIS_PORT_ENV_VAR)));
        settings.setRabbitmqHost(System.getenv(RABBITMQ_HOST_ENV_VAR) == null ? "localhost" : System.getenv(RABBITMQ_HOST_ENV_VAR));
        settings.setRabbitmqPort(System.getenv(RABBITMQ_PORT_ENV_VAR) == null ? 5672 : Integer.parseInt(System.getenv(RABBITMQ_PORT_ENV_VAR)));

        // if the security is enabled then all must be set - otherwise no security is happening
        if (System.getenv(KAFKA_SECURITY_PROTOCOL_ENV_VAR) != null) {
            if (System.getenv(KAFKA_SASL_MECHANISM_ENV_VAR) == null || System.getenv(KAFKA_SASL_JAAS_CONFIG_ENV_VAR) == null || System.getenv(KAFKA_SECURITY_PROTOCOL_ENV_VAR) == null) {
                throw new RuntimeException("if security is set up all 3 security attributes must be specified");
            }

            settings.setKafkaSecurityProtocol(System.getenv(KAFKA_SECURITY_PROTOCOL_ENV_VAR));
            settings.setKafkaSaslMechanism(System.getenv(KAFKA_SASL_MECHANISM_ENV_VAR));
            settings.setKafkaSaslJaasConfig(System.getenv(KAFKA_SASL_JAAS_CONFIG_ENV_VAR));
        }

        return settings;
    }
}
