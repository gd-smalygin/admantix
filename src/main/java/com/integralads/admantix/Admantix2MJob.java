package com.integralads.admantix;

import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Usage: hadoop jar target/admantix-0.0.1-SNAPSHOT-jar-with-dependencies.jar 
 *           com.integralads.admantix.Admantix2MJob N inputDir outputDir
 *   
 * Example:
 *   N= 10
 *   inputDir= /tmp/pages - dir with files to select N random lines from
 *   outputDir= /tmp/o - will be deleted 
 */
public class Admantix2MJob extends Configured implements Tool {
    
    @Override
    public int run(String[] args) throws Exception {
        
        long N = Long.parseLong(args[0]);
        
        Configuration conf = new Configuration();
        conf.setLong("N", N);
        Job job = Job.getInstance(conf, "sorter");
        
        Path outputPath = new Path(args[2]);
        FileSystem.get(conf).delete(outputPath, true);
        
        job.setJarByClass(Admantix2MJob.class);
        job.setMapperClass(AdmantixMapper.class);
        job.setReducerClass(AdmantixReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(LongWritable.Comparator.class);
        job.setReduceSpeculativeExecution(false);
        
        job.setNumReduceTasks(1);
        
        FileInputFormat.addInputPaths(job, args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        MultipleOutputs.addNamedOutput(job, "admantix", TextOutputFormat.class, Text.class, NullWritable.class);

        Date startTime = new Date();
        System.out.println("Job started: " + startTime);
        job.waitForCompletion(true);
        Date end_time = new Date();
        System.out.println("Job ended: " + end_time);
        System.out.println("The job took " + (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
        return 0;
      }

      private static class AdmantixMapper extends Mapper<Object, Text, LongWritable, Text> {
          
          @Override
          public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
              context.write(new LongWritable(RandomUtils.nextLong()), value);
          }
      }


      private static class AdmantixReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
          
          MultipleOutputs<Text, NullWritable> outputs;
          long N;
          long counter = 0;
          
          @Override
          protected void setup(Reducer<LongWritable, Text, Text, NullWritable>.Context context)
                throws IOException, InterruptedException {
              outputs = new MultipleOutputs<Text, NullWritable>(context);
              N=context.getConfiguration().getLong("N", 0);
          }
          
          @Override
          public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
              for (Text value : values) {
                  if (counter < N) {
                      outputs.write("admantix", value, NullWritable.get());
                      counter++;
                  } 
              }
          }
          
          @Override
          protected void cleanup(Reducer<LongWritable, Text, Text, NullWritable>.Context context)
                throws IOException, InterruptedException {
              outputs.close();
          }
          
      }
      
      public static void main(String[] args) throws Exception {
          int res = ToolRunner.run(new Configuration(), new Admantix2MJob(), args);
          System.exit(res);
        }
      
}
