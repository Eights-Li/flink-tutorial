package com.eights.sensor.bean

/**
 * temperatures sensor
 * @param id sensor id
 * @param timestamp timestamp
 * @param temperature temperature
 */
case class SensorReading(id: String,
                         timestamp: Long,
                         temperature: Double)
