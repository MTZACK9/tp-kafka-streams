package ma.enset;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Properties;

public class MeteorologicalData {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("application.id", "analysis-meteorological-data");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> src = builder.stream("weather-data");

        KStream<String,String> filteredData = src.filter((key, value) -> Double.parseDouble(value.split(",")[1]) > 30);

        KStream<String, String> toFahrenheit = filteredData.mapValues(value -> {
           String[] values = value.split(",");
           double fahrenheit = Double.parseDouble(values[1])*1.8+32;
           return values[0]+","+fahrenheit+","+values[2];
        });

        KGroupedStream<String, String> groupedStations = toFahrenheit.groupBy((key, value) -> value.split(",")[0]);
        KTable<String, String> aggByStation = groupedStations.aggregate(
                () -> "0,0,0",
                (key, value, agg) -> {
                    String[] values = value.split(",");
                    double temp = Double.parseDouble(values[1]);
                    double humidity = Double.parseDouble(values[2]);

                    String[] aggParts = agg.split(",");
                    double sumTemp = Double.parseDouble(aggParts[0]);
                    double sumHumidity = Double.parseDouble(aggParts[1]);
                    long count = Long.parseLong(aggParts[2]);

                    sumTemp += temp;
                    sumHumidity += humidity;
                    count++;

                    return sumTemp + "," + sumHumidity + "," + count;
                },
                Materialized.with(Serdes.String(), Serdes.String())
        );

        KStream<String, String> result = aggByStation.toStream().map((station,agg) ->{
            String[] values = agg.split(",");
            double avgTemp = Double.parseDouble(values[0])/Double.parseDouble(values[2]);
            double avgHumidity = Double.parseDouble(values[1])/Double.parseDouble(values[2]);
            String formattedResult = String.format("%s : Température Moyenne = %.2f°F, Humidité Moyenne = %.1f%%",
                    station, avgTemp, avgHumidity);
            return KeyValue.pair(station, formattedResult);
        });

        result.to("station-averages");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
