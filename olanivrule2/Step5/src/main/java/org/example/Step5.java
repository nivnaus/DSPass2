package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class Step5 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text mapKey = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] parsed = value.toString().split("\t");
            String trio = parsed[0];
            String varAndVal = parsed[1];
            mapKey.set(trio);
            context.write(mapKey, new Text(varAndVal));
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,DoubleWritable> {

        @Override // c2#65, c1#78, n3#689
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            String trio = key.toString();
            String[] parsedTrio = trio.split("#");
            String w1 = parsedTrio[0];
            String w2 = parsedTrio[1];
            String w3 = parsedTrio[2];
            double c0 = 0;
            double c1 = 0;
            double c2 = 0;
            double n1 = 0;
            double n2 = 0;
            double n3 = 0;

            Iterator<Text> iterator = values.iterator();
            while(iterator.hasNext()) {
                Text value = iterator.next();
                String var = value.toString().split("#")[0];
                int val = Integer.parseInt(value.toString().split("#")[1]);

                switch (var) {
                    case "c0": { c0 = val;break;}
                    case "c1": { c1 = val; break;}
                    case "c2": { c2 = val; break;}
                    case "n1": { n1 = val; break;}
                    case "n2": { n2 = val; break;}
                    case "n3": { n3 = val; break;}
                }
            }

            double k2 = (Math.log(n2 + 1) + 1) / (Math.log(n2 + 1) + 2);
            double k3 = (Math.log(n3 + 1) + 1) / (Math.log(n3 + 1) + 2);

            double probability = k3 * (n3 / c2) + (1 - k3) * k2 * (n2 / c1) + (1 - k3) * (1 - k2) * (n1 / c0);
            double roundedValue = Math.round(probability * 100.0) / 100.0;

            Text trioWithSpaces = new Text(w1 + " " + w2 + " " +w3);
            context.write(trioWithSpaces, new DoubleWritable(roundedValue));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 5 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 5");
        job.setJarByClass(Step5.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.addInputPath(job, new Path("s3://nivolarule29122024/constsW2.txt"));
        TextInputFormat.addInputPath(job, new Path("s3://nivolarule29122024/constsW3.txt"));
        TextInputFormat.addInputPath(job, new Path("s3://nivolarule29122024/constsWithC0.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://nivolarule29122024/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
