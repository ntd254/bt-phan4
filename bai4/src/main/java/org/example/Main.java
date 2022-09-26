package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;


/* Schema
Table: log
Row key: guid
column family: url, ip, cookieCreate, otherInfo
Ex: put 'log', '1234', 'url:2020-09-10 10-24-30', 'https://kenh14/tin-the-thao'*/



public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        urlOfGuid("12345", "2020-09-10");
        mostIPOfGuid("12345");
        mostRecentlyTimeCreate("12345");
        diffTimeCreateCookie();
    }

    public static void urlOfGuid(String guid, String date) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "localhost");
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Table table = connection.getTable(TableName.valueOf("log"));
            Get get = new Get(guid.getBytes());
            get.addFamily("url".getBytes());
            Result rs = table.get(get);
            NavigableMap<byte[], byte[]> urlMap = rs.getFamilyMap("url".getBytes());
            System.out.println("url:");
            urlMap.forEach((key, value) -> {
                if (new String(key).contains(date)) {
                    System.out.println(new String(key) + " " + new String(value));
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void mostIPOfGuid(String guid) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "localhost");
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Table table = connection.getTable(TableName.valueOf("log"));
            Get get = new Get(guid.getBytes());
            get.addFamily("ip".getBytes());
            Result rs = table.get(get);
            NavigableMap<byte[], byte[]> ipMap = rs.getFamilyMap("ip".getBytes());
            HashMap<String, Integer> numEachIp = new HashMap<>();
            ipMap.forEach((key, value) -> {
                numEachIp.merge(new String(value),1, (oldValue, increment) -> oldValue + increment);
            });
            ArrayList<String> listIp = new ArrayList<>(numEachIp.keySet());
            listIp.sort((ip1, ip2) -> {
                return numEachIp.get(ip2) - numEachIp.get(ip1);
            });
            System.out.println("ip num:");
            listIp.forEach(ip -> System.out.println(ip + " " + numEachIp.get(ip)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void mostRecentlyTimeCreate(String guid) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "localhost");
        LocalDateTime mostRecentlyTime;
        LocalDateTime now = LocalDateTime.now();
        long leastDiff;
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Table table = connection.getTable(TableName.valueOf("log"));
            Get get = new Get(guid.getBytes());
            get.addFamily("url".getBytes());
            Result rs = table.get(get);
            NavigableMap<byte[], byte[]> urlMap = rs.getFamilyMap("url".getBytes());
            mostRecentlyTime = LocalDateTime.parse(new String(urlMap.firstKey()), DateTimeFormatter.ofPattern("yyyy-MM-dd HH-mm-ss"));
            leastDiff = Duration.between(mostRecentlyTime, now).toMillis();
            for (Map.Entry<byte[], byte[]> entry : urlMap.entrySet()) {
                LocalDateTime timeCreate = LocalDateTime.parse(new String(entry.getKey()), DateTimeFormatter.ofPattern("yyyy-MM-dd HH-mm-ss"));
                if (Duration.between(timeCreate, now).toMillis() < leastDiff) mostRecentlyTime = timeCreate;
            }
            System.out.println("timeCreate:");
            System.out.println(mostRecentlyTime);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void diffTimeCreateCookie() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "localhost");
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Table table = connection.getTable(TableName.valueOf("log"));
            Scan scan = new Scan();
            scan.addFamily("cookieCreate".getBytes());
            ResultScanner rs = table.getScanner(scan);
            System.out.println("Guid:");
            for (Result result = rs.next(); result != null; result = rs.next()) {
                NavigableMap<byte[], byte[]> cookieCreateMap = result.getFamilyMap("cookieCreate".getBytes());
                for (Map.Entry<byte[], byte[]> entry : cookieCreateMap.entrySet()) {
                    LocalDateTime cookieCreate = LocalDateTime.parse(new String(entry.getValue()), DateTimeFormatter.ofPattern("yyyy-MM-dd HH-mm-ss"));
                    LocalDateTime timeCreate = LocalDateTime.parse(new String(entry.getKey()), DateTimeFormatter.ofPattern("yyyy-MM-dd HH-mm-ss"));
                    if (Duration.between(cookieCreate, timeCreate).toMinutes() < 30) System.out.println(new String(result.getRow()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}