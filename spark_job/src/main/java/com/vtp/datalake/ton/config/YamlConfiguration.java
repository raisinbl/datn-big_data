package com.vtp.datalake.ton.config;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

public class YamlConfiguration extends Configuration {

    public HashMap<String, Object> map = new HashMap<>();
    private static final Logger LOGGER = LogManager.getLogger(YamlConfiguration.class);

    private String yamlConfigFile;

    public YamlConfiguration() {
        this(true);
    }

    public YamlConfiguration(boolean productMode) {

        if (productMode) {
            yamlConfigFile = "product.yaml";
        } else {
            yamlConfigFile = "config/dev.yaml";
        }
        try {
            load();
        } catch (Exception e) {
            LOGGER.error("Load config error", e);
        }

    }

    @Override
    public void load() throws IOException {
        InputStream inputStream = new FileInputStream(new File(yamlConfigFile));
        Yaml yaml = new Yaml();
        map = yaml.load(inputStream);
    }

    @Override
    public void loadFrom(String fileConfig) throws IOException {
        this.yamlConfigFile = fileConfig;
        load();
    }

    @Override
    public String getConfig(String key) {
        return String.valueOf(map.get(key));
    }

    public static void main(String[] args) throws IOException {
        Configuration configuration = ConfigurationFactory.createYamlConfig();
        configuration.loadFrom("config/product.yaml");
    }
}

