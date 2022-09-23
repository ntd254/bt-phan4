package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {


    public static void main(String[] args) {
        getEmployeeManagerFromDepartment("d001");
        getSalary("1987-12");
        getTitleOfEmployee("10017");
    }

    public static void getEmployeeManagerFromDepartment(String departmentId) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "localhost");
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Table table = connection.getTable(TableName.valueOf("departments"));
            Get get = new Get(departmentId.getBytes());
            Result rs = table.get(get);
            NavigableMap<byte[], byte[]> managerMap = rs.getFamilyMap("manager".getBytes());
            System.out.println("manager:");
            managerMap.forEach((key, value) -> {
                System.out.println(new String(key) + " " + new String(value));
            });
            NavigableMap<byte[], byte[]> employeeMap = rs.getFamilyMap("employee".getBytes());
            System.out.println("Employee:");
            employeeMap.forEach((key, value) -> {
                System.out.println(new String(key) + " " + new String(value));
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getSalary(String monthYear) {
        AtomicInteger total = new AtomicInteger();
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "localhost");
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Table table = connection.getTable(TableName.valueOf("employee"));
            Scan scan = new Scan();
            scan.addFamily("salary".getBytes());
            ResultScanner rs = table.getScanner(scan);
            for (Result result = rs.next(); result != null; result = rs.next()) {
                NavigableMap<byte[], byte[]> managerMap = result.getFamilyMap("salary".getBytes());
                managerMap.forEach((key, value) -> {
                    if (new String(value).contains(monthYear)) total.addAndGet(Integer.parseInt(new String(key)));
                });
            }
            System.out.println("total salary in " + monthYear + ": " + total.get());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getTitleOfEmployee(String employeeId) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "localhost");
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Table table = connection.getTable(TableName.valueOf("employee"));
            Get get = new Get(employeeId.getBytes());
            Result rs = table.get(get);
            NavigableMap<byte[], byte[]> managerMap = rs.getFamilyMap("title".getBytes());
            System.out.println("title:");
            managerMap.forEach((key, value) -> {
                System.out.println(new String(key) + " " + new String(value));
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}