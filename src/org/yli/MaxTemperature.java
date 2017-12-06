package org.yli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;

import java.io.IOException;

public class MaxTemperature {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        JobConf jobConf = new JobConf(new Configuration(), MaxTemperature.class);

        jobConf.setJarByClass(MaxTemperature.class);
        jobConf.setJobName("Max Temperature");

        FileInputFormat.addInputPath(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        jobConf.setMapperClass(MaxTemperatureMapper.class);
        jobConf.setReducerClass(MaxTemperatureReducer.class);

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        Job job = new Job(jobConf);
        System.exit(job.getJob().waitForCompletion(true) ? 0 : 1);
    }

}
