package com.vtp.datalake.ton.config;

import java.io.IOException;
import java.io.Serializable;

public abstract class Configuration implements Serializable {

    public abstract void load() throws IOException;

    public abstract void loadFrom(String fileConfig) throws IOException;

    public abstract String getConfig(String key);

    public Integer getConfigInt(String key) {
        return Integer.valueOf(getConfig(key));
    }


    public Long getConfigLong(String key) {
        return Long.valueOf(getConfig(key));
    }

    public Boolean getConfigBoolean(String key) {
        return Boolean.valueOf(getConfig(key));
    }

    public Float getConfigFloat(String key) {
        return Float.valueOf(getConfig(key));
    }

    public Double getConfigDouble(String key) {
        return Double.valueOf(getConfig(key));
    }
}
