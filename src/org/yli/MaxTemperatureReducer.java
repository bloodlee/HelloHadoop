package org.yli;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class MaxTemperatureReducer implements Reducer<Text, IntWritable, Text, IntWritable> {

//    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
//            throws IOException, InterruptedException {
//        int maxValue = Integer.MIN_VALUE;
//
//        for (IntWritable value : values) {
//            maxValue = Math.max(maxValue, value.get());
//        }
//        context.write(key, new IntWritable(maxValue));
//    }

    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> context, Reporter reporter)
            throws IOException {
        int maxValue = Integer.MIN_VALUE;

        while (values.hasNext()) {
            maxValue = Math.max(maxValue, values.next().get());
        }
        context.collect(key, new IntWritable(maxValue));
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf jobConf) {

    }
}
