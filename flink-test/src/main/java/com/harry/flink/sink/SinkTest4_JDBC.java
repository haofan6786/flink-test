package com.harry.flink.sink;

import com.harry.flink.beans.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkTest4_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<String> inputStream = env.readTextFile("/Users/harry/IdeaProjects/flink-test/flink-test/src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.addSink(new RichSinkFunction<SensorReading>() {
            Connection connection = null;
            PreparedStatement insertStmt = null;
            PreparedStatement updateStmt = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection("jdbc:mysql://localhost/harry","${user}","${password}");
                insertStmt = connection.prepareStatement("insert into sensor_temp (id,temp) values (?,?)");
                updateStmt = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
            }

            @Override
            public void invoke(SensorReading value, Context context) throws Exception {
                //直接执行更新语句，如果没有更新那么久插入
                updateStmt.setDouble(1,value.getTemperature());
                updateStmt.setString(2,value.getId());
                updateStmt.execute();
                if (updateStmt.getUpdateCount()==0){
                    insertStmt.setString(1,value.getId());
                    insertStmt.setDouble(2,value.getTemperature());
                    insertStmt.execute();
                }
            }

            @Override
            public void close() throws Exception {
                updateStmt.close();
                insertStmt.close();
                connection.close();
            }
        });

        env.execute();
    }
}
