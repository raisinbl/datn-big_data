package com.vtp.datalake.ton.config;

import java.sql.Connection;
import java.sql.DriverManager;

import static com.vtp.datalake.ton.config.ConfigurationFactory.getConfigInstance;


public class PhoenixConnectionFactory {

    private static PhoenixConnectionFactory ourInstance;

    public static PhoenixConnectionFactory getInstance() {
        if (ourInstance == null) {
            synchronized (PhoenixConnectionFactory.class) {
                if (ourInstance == null) {
                    ourInstance = new PhoenixConnectionFactory();
                }
            }
        }
        return ourInstance;
    }

    public PhoenixConnectionFactory() {
    }

    public Connection getPhoenixConnection() throws Exception {
        Configuration config;
        config = getConfigInstance();

        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        return DriverManager.getConnection(config.getConfig("PHOENIX.URL"));
    }
}
