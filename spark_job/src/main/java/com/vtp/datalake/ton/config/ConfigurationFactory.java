package com.vtp.datalake.ton.config;

public class ConfigurationFactory {


    private static Configuration config;
    public static Configuration createYamlConfig(){
        String mode = System.getenv("CTW_ENV");
        if(mode == null){
            return new YamlConfiguration(false);
        }
        
        if (mode.equalsIgnoreCase("product"))
            return new YamlConfiguration(true);
        else
            return new YamlConfiguration(false);
    }

    public static Configuration getConfigInstance(){
        if (config == null){
            config = createYamlConfig();
        }
        return config;
    }
}
