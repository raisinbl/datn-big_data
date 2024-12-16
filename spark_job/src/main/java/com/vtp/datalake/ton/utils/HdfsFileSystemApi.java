package com.vtp.datalake.ton.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsFileSystemApi {
    private static FileSystem fs;

    public static FileSystem initFileSystem(){
        Configuration configuration = new Configuration();
        configuration.addResource(new Path("config/hdfs-site.xml"));
        configuration.addResource(new Path("config/core-site.xml"));
        try {
            return FileSystem.get(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static FileSystem getInstance(){
        if(fs == null){
            fs = initFileSystem();
        }
        return fs;
    }

}
