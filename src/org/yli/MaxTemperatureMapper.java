package org.yli;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class MaxTemperatureMapper implements Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable longWritable, Text value, OutputCollector<Text, IntWritable> context, Reporter reporter) throws IOException {
        String line = value.toString();
        String year = line.substring(15, 19);

        int airTemp;
        if (line.charAt(87) == '+') {
            airTemp = Integer.parseInt(line.substring(88, 92));
        } else {
            airTemp = Integer.parseInt(line.substring(87, 92));
        }

        String quality = line.substring(92, 93);

        if (airTemp != MISSING && quality.matches("[01459]")) {
            context.collect(new Text(year), new IntWritable(airTemp));
        }
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf jobConf) {

    }

//    @Override
//    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        String line = value.toString();
//        String year = line.substring(15, 19);
//
//        int airTemp;
//        if (line.charAt(87) == '+') {
//            airTemp = Integer.parseInt(line.substring(88, 92));
//        } else {
//            airTemp = Integer.parseInt(line.substring(87, 92));
//        }
//
//        String quality = line.substring(92, 93);
//
//        if (airTemp != MISSING && quality.matches("[01459]")) {
//            context.write(new Text(year), new IntWritable(airTemp));
//        }
//    }
}
