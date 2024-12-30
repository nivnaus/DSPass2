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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Step1 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
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
                "אבל", "א"
        ));

        @Override//3gram: w1 w2 w3\tyear\tmatch_count\tpage_count\tvolume_count
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            // parse 3gram input line
            String[] tabParse = value.toString().split("\t");
            String trio = tabParse[0];
            String trioMatchCount = tabParse[2];
            int trioMatchCountInt = Integer.parseInt(trioMatchCount);
            String[] words = trio.split(" ");
            String w1 = words[0];
            String w2 = words[1];
            String w3 = words[2];

            // extract the 5 subs -> filtering stop words
            if(!(stopWords.contains(w1) || stopWords.contains(w2)  || stopWords.contains(w3))) {
                // emit the 5 subs
                mapKey.set(w1+"#"+w2+"#"+w3);
                context.write(mapKey, new Text( trioMatchCountInt + "\t" + trio)); //N3
                mapKey.set("*#"+w2+"#"+w3);
                Text oneTrio = new Text("1\t" + trio);// trio = "w1 w2 w3"
                context.write(mapKey, oneTrio); // N2
                mapKey.set("*#"+w2+"#*");
                context.write(mapKey, oneTrio); // C1
                mapKey.set("*#*#"+w3);
                context.write(mapKey, oneTrio); // N1
                mapKey.set(w1 + "#" + w2 + "#*");
                context.write(mapKey, oneTrio); //C2
                mapKey.set(w1 + "#*#*");
                context.write(mapKey, oneTrio); //for C0 in step 2
            }
        }
    }


    public static class ReducerClass extends Reducer<Text,Text,Text,IntWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
//            int sum = 0;
//            for (Text value : values) {
//                String[] parts = value.toString().split("\t");
//                int trioSum = Integer.parseInt(parts[0]); // 1\tw1 w2 w3
//                sum += trioSum;
//            }
//
//            context.write(key, new IntWritable(sum));
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
        @Override // *#halah#* [1\tyeled halah lagan, 1\tyeled halah habaita]
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
                String[] parts = value.toString().split("\t");
                int count = Integer.parseInt(parts[0]); // Parse the count from the Text value
                sum += count;
            }
            // Emit the summed-up value as Text
            context.write(key, new Text(sum + ""));
        }
    }
    // *#halah#* [1] (reducer 1) -> r0001   *#halah#*\tc2 = 1
    // *#halah#* [1] (reducer 2) -> r0002   *#halah#*\tc2 = 1
    // yeled halah lagan ->  key: *#halah#*  value: 1\tyeled halah lagan
    // yeled halah habaita -> key: *#halah#* value: 1\tyeled halah habaita

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String splitValue = value.toString().split("\t")[1];// 1\tw1 w2 w3

            return Math.abs(splitValue.hashCode()) % numPartitions;
        }
    }
    // TODO: we need to change it for our assignment
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 1");
        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
        // todo: start with a smaller file
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
        TextInputFormat.addInputPath(job, new Path("s3://nivolarule29122024/exampleOf3gram.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://nivolarule29122024/subSums.txt"));// TODO: change this to our own bucket
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
