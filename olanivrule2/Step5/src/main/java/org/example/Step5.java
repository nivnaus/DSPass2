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
import java.util.Iterator;

public class Step5 {
    public static class MapperClass extends Mapper<LongWritable, Text, TrioProbKey, Text> {
        private final static IntWritable one = new IntWritable(1);
        private TrioProbKey mapKey = new TrioProbKey();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] parsed = value.toString().split("\t");// "w1 w2 w3\tprob"
            String[] trio = parsed[0].split(" ");
            String w1 = trio[0];
            String w2 = trio[1];
            String w3 = trio[2];
            Double prob = Double.parseDouble(parsed[1]);

            mapKey.setW1(w1);
            mapKey.setW2(w2);
            mapKey.setW3(w3);
            mapKey.setNumber(prob);

            context.write(mapKey, new Text(""));
        }
    }

    public static class ReducerClass extends Reducer<TrioProbKey,Text,Text,DoubleWritable> {

        @Override // c2#65, c1#78, n3#689
        public void reduce(TrioProbKey key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            // "w1 w2 w3\tprob"
            context.write(new Text(key.getTrio()), new DoubleWritable(key.getNumber()));
        }
    }

    public static class PartitionerClass extends Partitioner<TrioProbKey, Text> {
        @Override
        public int getPartition(TrioProbKey key, Text value, int numPartitions) {
            return Math.abs(key.toString().hashCode()) % numPartitions;
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
        job.setMapOutputKeyClass(TrioProbKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.addInputPath(job, new Path("s3://nivolarule29122024/notSortedOutput.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://nivolarule29122024/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
