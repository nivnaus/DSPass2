package org.example;

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

import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Step1 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text mapKey = new Text();
        private HashSet<String> stopWords = new HashSet<>(Arrays.asList(
                "״", "׳", "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד",
                "מן", "מכל", "מי", "מהם", "מה", "מ", "למה", "לכל", "לי", "לו",
                "להיות", "לה", "לא", "כן", "כמה", "כלי", "כל", "כי", "יש",
                "ימים", "יותר", "יד", "י", "זה", "ז", "ועל", "ומי", "ולא",
                "וכן", "וכל", "והיא", "והוא", "ואם", "ו", "הרבה", "הנה", "היו",
                "היה", "היא", "הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו",
                "בה", "בא", "את", "אשר", "אם", "אלה", "אל", "אך", "איש", "אין",
                "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":", "1",
                ".", "-", "*", "\"", "!", "שלשה", "בעל", "פני", ")", "גדול",
                "שם", "עלי", "עולם", "מקום", "לעולם", "לנו", "להם", "ישראל",
                "יודע", "זאת", "השמים", "הזאת", "הדברים", "הדבר", "הבית",
                "האמת", "דברי", "במקום", "בהם", "אמרו", "אינם", "אחרי", "אותם",
                "אדם", "(", "חלק", "שני", "שכל", "שאר", "ש", "ר", "פעמים",
                "נעשה", "ן", "ממנו", "מלא", "מזה", "ם", "לפי", "ל", "כמו",
                "כבר", "כ", "זו", "ומה", "ולכל", "ובין", "ואין", "הן", "היתה",
                "הא", "ה", "בל", "בין", "בזה", "ב", "אף", "אי", "אותה", "או",
                "אבל", "א", " ", "#", "$", "%", "&", "'", "+", ",", "/"
        ));

        @Override//3gram: w1 w2 w3\tyear\tmatch_count\tpage_count\tvolume_count
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            // parse 3gram input line
            System.out.println(value);
            String[] tabParse = value.toString().split("\t");
            String trio = tabParse[0];
            String trioMatchCount = tabParse[2];
            IntWritable trioMatchCountInt = new IntWritable(Integer.parseInt(trioMatchCount));
            String[] words = trio.split(" ");
            if(words.length == 3) {
                String w1 = words[0];
                String w2 = words[1];
                String w3 = words[2];

                // extract the 5 subs -> filtering stop words
                if (!(stopWords.contains(w1) || stopWords.contains(w2) || stopWords.contains(w3))) {
                    // emit the 5 subs
                    mapKey.set(w1 + "#" + w2 + "#" + w3);
                    context.write(mapKey, trioMatchCountInt); //N3
                    mapKey.set("*#" + w2 + "#" + w3);
                    context.write(mapKey, trioMatchCountInt); // N2
                    mapKey.set("*#" + w2 + "#*");
                    context.write(mapKey, trioMatchCountInt); // C1
                    mapKey.set("*#*#" + w3);
                    context.write(mapKey, trioMatchCountInt); // N1
                    mapKey.set(w1 + "#" + w2 + "#*");
                    context.write(mapKey, trioMatchCountInt); //C2
                    IntWritable trioMatchCountIntTimes3 = new IntWritable(3 * Integer.parseInt(trioMatchCount));
                    mapKey.set("*#*#*");
                    context.write(mapKey, trioMatchCountIntTimes3); //for C0 in step 2

                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            // if not 3 *
            if(!key.toString().equals("*#*#*")){
                context.write(key, new IntWritable(sum));
            }
            //if 3 * , upload to s3 to "c0.txt"
            else {
                Configuration conf = context.getConfiguration();
                String s3Path = "s3://nivolarule29122024/c0.txt";

                Path path = new Path(s3Path);
                FileSystem fs = FileSystem.get(path.toUri(), conf); // Automatically uses the Hadoop configuration

                try (OutputStream outputStream = fs.create(path);
                    PrintWriter writer = new PrintWriter(outputStream)) {
                    writer.println(sum); // Write the value of c0
                }
            }
        }
    }



    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 1");
        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));

        // start with a smaller file
//        TextInputFormat.addInputPath(job, new Path("s3://nivolarule29122024/exampleOf3gram.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://nivolarule29122024/subSums.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}