package com.harry.flink.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@Data
@ToString
public class SensorReading {
    private String id;
    private Long timestamp;
    private Double temperature;
}
