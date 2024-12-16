package com.vtp.datalake.ton.utils;

import com.vtp.datalake.ton.config.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;

import static com.vtp.datalake.ton.config.ConfigurationFactory.getConfigInstance;

public class Utils {
    private static Configuration config;
    private DateTimeFormatter timefm;
    private DateTimeFormatter datefm;
    private DateTimeFormatter datefmv2;
    public DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    private static final Logger LOGGER = LogManager.getLogger(Utils.class);


    public Utils() {
        this.timefm = DateTimeFormatter.ofPattern("HHmmss");
        this.datefm = DateTimeFormatter.ofPattern("uuuuMMdd");
        this.datefmv2 = DateTimeFormatter.ofPattern("uuuu-MM-dd");
        this.config = getConfigInstance();
    }

    public String getDateNow() {
        return this.datefm.format(LocalDate.now());
    }

    public String getLocalTimeNow() {
        return this.timefm.format(LocalTime.now());
    }

    public double avgDate(LocalDate date) {
        int numDates = date.getDayOfMonth();
        int countSAT = 0;
        int countSUN = 0;
        for (int i = 0; i < numDates; i++) {
            LocalDate temp = date.minusDays(i);
            if (temp.getDayOfWeek().toString().equals("SATURDAY")) {
                countSAT += 1;
            }
            if (temp.getDayOfWeek().toString().equals("SUNDAY")) {
                countSUN += 1;
            }
        }
        double avg = (numDates - countSAT - countSUN) * Float.parseFloat(config.getConfig("TBNGAY.NT"))
                + countSAT * Float.parseFloat(config.getConfig("TBNGAY.T7")) +
                countSUN * Float.parseFloat(config.getConfig("TBNGAY.CN"));
        return avg;
    }

    public String[] getURLDateRange(String URL, int numDates) {
        ArrayList<String> listPath = new ArrayList<>();
        for (int i = 0; i < numDates; i++) {
            String path = URL + "/partition=" + this.minusDays(LocalDate.now(), i);
            try {
                if (HdfsFileSystemApi.getInstance().exists(new Path(path))) {
                    listPath.add(path);
                }
            } catch (IOException e) {
                LOGGER.error(path + " Not Found");
            }
        }
        return listPath.toArray(new String[0]);
    }

    public String[] checkSizeFolder(String URL) throws IOException {

        ArrayList<String> listPath = new ArrayList<>();
        URL = URL.endsWith("*") ? URL.replace("*", ""): URL;

        String path = URL + "/partition=" + this.minusDays(LocalDate.now(), 1);
        System.out.println(path);

        Long checksize = HdfsFileSystemApi.getInstance()
                .getContentSummary(new Path(path))
                .getSpaceConsumed();
        return new String[]{String.valueOf(checksize)};
    }

    public boolean checkCachePartition(String pathCache) throws IOException {
        if (HdfsFileSystemApi.getInstance().exists(new Path(pathCache))) {
            if (HdfsFileSystemApi.getInstance().getContentSummary(new Path(pathCache)).getLength()/1024 > 1) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    public String getUrlLatestDayCache(String URL){
        URL = URL.endsWith("*") ? URL.replace("*", ""): URL;
        String resultPath = null;
        for (int i = 0; i < 999 ; i++){
            String subPath = URL.endsWith("/") || URL.endsWith("//") ? "partition=" : "/partition=";
            String path = URL + subPath + this.minusDays(LocalDate.now(), i).replace("-", "");
            try {
                if (checkCachePartition(path)) {
                    System.out.println("LOAD CACHE WITH PATH: " + path);
                    resultPath = path;
                    break;
                }
//                else {
//                    if(i >= 0){
//                        NotifyUtil.sendNotify("Error when get data cache from path: " + path + "; Error in " + i + " partition from now");
//                    }
//                }
            } catch (IOException e) {
                // raise exception
                IOException ex = new IOException(path + " Not Found!");
            }
        }
        return resultPath;
    }

    public String getUrlLatestDay(String URL){
        URL = URL.endsWith("*") ? URL.replace("*", ""): URL;
        for (int i = 0; i < 999 ; i++){
            String subPath = URL.endsWith("/") || URL.endsWith("//") ? "partition=" : "/partition=";
            String path = URL + subPath + this.minusDays(LocalDate.now(), i).replace("-", "");
            try {
                if (HdfsFileSystemApi.getInstance().exists(new Path(path)))
                    return path;
            } catch (IOException e) {
                LOGGER.error(path + " Not Found!");
            }
            System.out.println(path);
        }

        return null;
    }

    public String getDDMMYYYY(LocalDate date, int numDates, int numMonths, int numYears) {
        return this.datefmv2.format(date.minusDays(numDates).minusMonths(numMonths).minusYears(numYears));
    }

    public Duration getDiffDateTime(String dt1, String dt2) {
        /**
         * Diff 2 datetime with format "yyyy-MM-dd HH:mm:ss"
         */
        LocalDateTime dateTime1 = LocalDateTime.parse(dt1, formatter);
        LocalDateTime dateTime2 = LocalDateTime.parse(dt2, formatter);
        return Duration.between(dateTime1, dateTime2);

    }


    public LocalDateTime getDateTime(String dateTime) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            return LocalDateTime.parse(dateTime, formatter);
        } catch (Exception e) {
            return null;
        }
    }

    public String getDateTimeWithResetTime(String datetime) {
        return getDateTime(datetime).with(LocalTime.MIN).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    public String getURLDate(String URL) {
        return URL + "/" + LocalDate.now();
    }

    public String getURLDateMinus1(String URL) {
        return minusDays(LocalDate.now(), 1);
    }

    public String minusDays(LocalDate date, int numDates) {
        return this.datefm.format(date.minusDays(numDates));
    }

    public LocalDate minusMonths(LocalDate date, int numMonths) {
        return date.minusMonths(numMonths);
    }

    public LocalDate minusYears(LocalDate date, int numYears) {
        return date.minusYears(numYears);
    }

    public long getTimeStamp(String dateTime) {
        Date parseDateTime;
        try {
            parseDateTime = dateTimeFormat.parse(dateTime);
            return parseDateTime.getTime();
        } catch (Exception e) {
            try {
                return Long.parseLong(dateTime);
            } catch (Exception f) {
                return 0;
            }
        }
    }


    public Timestamp moveToOtherTime(LocalDateTime localDateTime, int day, int hour, int minus) {
        long orgDateTime = getTimeStamp(localDateTimeToString(localDateTime));
        orgDateTime = orgDateTime + 24 * 3600 * 1000 * day + 3600 * 1000 * hour + 1000 * minus * 60;
        return new Timestamp(orgDateTime);
    }

    public String localDateTimeToString(LocalDateTime dateTime) {
        return dateTime.format(formatter);
    }

    public String[] getURLDateRangeGGSFormat(String URL, int numDates) {
        ArrayList<String> listPath = new ArrayList<>();
        for (int i = 0; i < numDates; i++) {
            String path = URL + "/partition=" + this.minusDays(LocalDate.now(), i);
            try {
                if (HdfsFileSystemApi.getInstance().exists(new Path(path))) {
                    listPath.add(path + "/*");
                }
            } catch (IOException e) {
                LOGGER.error(path + " Not Found");
            }

        }
        return listPath.toArray(new String[0]);
    }

    public String getURLDateGGSFormat(String URL) {
        return URL + "/partition=" + LocalDate.now() + "/*";
    }

    public long getCurrentTime() {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        return timestamp.getTime();
    }

    public String[] getURLDateRangeCombine(String URL1, int numDates1, String URL2, int numDates2){
        ArrayList<String> listPath = new ArrayList<>();
        URL1 = URL1.endsWith("*") ? URL1.replace("*", ""): URL1;
        URL2 = URL2.endsWith("*") ? URL2.replace("*", ""): URL2;
        for (int i = 0; i < numDates1 ; i++){
            String subPath = URL1.endsWith("/") || URL1.endsWith("//") ? "partition=" : "/partition=";
            String path = URL1 + subPath + this.minusDays(LocalDate.now(), i).replace("-", "");
            try {
                if (HdfsFileSystemApi.getInstance().exists(new Path(path)))
                    listPath.add(path);
            } catch (IOException e) {
                LOGGER.error(path + " Not Found!");
            }
        }

        for (int i = 0; i < numDates2 ; i++){
            String subPath = URL2.endsWith("/") || URL2.endsWith("//") ? "partition=" : "/partition=";
            String path = URL2 + subPath + this.minusDays(LocalDate.now(), i).replace("-", "");
            try {
                if (HdfsFileSystemApi.getInstance().exists(new Path(path)))
                    listPath.add(path);
            } catch (IOException e) {
                LOGGER.error(path + " Not Found!");
            }
        }
        return listPath.toArray(new String[0]);
    }


}
