package org.example;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;


import javax.naming.Context;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;

public class Step2 {
//    public static long c0 = 0;


    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text mapKey = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] parsed = value.toString().split("\t");
            String trio = parsed[0];
            int trioSum = Integer.parseInt(parsed[1]);
            mapKey.set(trio);
            context.write(mapKey, new IntWritable(trioSum));
        }
    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text,Text> {
        private HashMap<String, Integer> asteriskMap = new HashMap<>();// *#*#w3 *#w2#* w1#*#*

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int c0 = Integer.parseInt(context.getConfiguration().get("c0"));
            String trio = key.toString();
            String[] parsedTrio = trio.split("#");
            String w1 = parsedTrio[0];
            String w2 = parsedTrio[1];
            String w3 = parsedTrio[2];
            int freqOfTrio = values.iterator().next().get(); //values should have exactly one value
            System.out.println("trio:\t" + trio);
            System.out.println("freqOfTrio:\t" + freqOfTrio);
            if(w1.equals("*") || w3.equals("*")) {
                asteriskMap.put(trio,freqOfTrio);
            } else {
                //collect n2, c1, c2..
                int c1 = asteriskMap.get("*#"+w2+"#*");
                int c2 = asteriskMap.get(w1+ "#" + w2 + "#*");
                int n2 = asteriskMap.get("*#"+w2+"#"+w3);
                int n3 = freqOfTrio;

                context.write(key, new Text("c0#"+c0)); // todo: make sure it works
                context.write(key, new Text("c1#"+c1));
                context.write(key, new Text("c2#"+c2));
                context.write(key, new Text("n2#"+n2));
                context.write(key, new Text("n3#"+n3));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            // Partition by the second word in the trio
            String[] parsedTrio = key.toString().split("#");
            String w2 = parsedTrio[1];
            return Math.abs(w2.hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        //function: get c0 from s3://c0.txt
//        String s3Path = "s3a://nivolarule29122024/c0.txt";
//        FileSystem fs = FileSystem.get(conf);
//
//        // Create a Path object for the S3 file
//        Path filePath = new Path(s3Path);
//
//        // Read the file from S3
//        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)))) {
//            String line;
//            // Read the content of the file (which should contain c0 value)
//            while ((line = reader.readLine()) != null) {
//                // Assuming the file contains only the c0 value
//                String c0 = line.trim();
//                System.out.println("Extracted c0 value: " + c0);
//
//                // You can now use the c0 value in your Step 2 processing logic
//                // For example, pass it to the next mapper/reducer, etc.
//                conf.set("c0", c0);
//            }
//        } catch (IOException e) {
//            System.err.println("Error reading file from S3: " + e.getMessage());
//        }

        // Create S3 client (AWS SDK will automatically pick up credentials)
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion("us-east-1")  // Specify the region your bucket is in
                .build();

        // Specify your S3 bucket and object key
        String bucketName = "nivolarule29122024";
        String key = "c0.txt";

        // Read file from S3
        try {
            S3Object s3Object = s3Client.getObject(bucketName, key);
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
            String line;
            while ((line = reader.readLine()) != null) {
                String c0 = line.trim();  // Assuming c0 is the only content in the file
                System.out.println("Extracted c0 value: " + c0);
                conf.set("c0", c0);
            }
            reader.close();
        } catch (IOException e) {
            System.err.println("Error reading from S3: " + e.getMessage());
        }



        Job job = Job.getInstance(conf, "Step 2");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, new Path("s3://nivolarule29122024/subSums.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://nivolarule29122024/constsW2.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
