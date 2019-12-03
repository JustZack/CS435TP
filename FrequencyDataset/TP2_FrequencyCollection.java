import java.io.*;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class TP2_FrequencyCollection {
    
    public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {
        
        private IntWritable one = new IntWritable(1);
        
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {
            
            if (value == null || value.toString().trim().isEmpty())
                return;
            
            String val = value.toString();
            val = val.replaceAll("\\\\u2014|\\\\u2013", " ");
            val = val.replaceAll("\\\\\"", "\"");
            val = val.replaceAll("&amp;|&", "and");
            val = val.replaceAll("!|\\?|:", ".");
            val = val.replaceAll("(\\n)+", ". ");
            val = val.replaceAll("-|—|–", " ");
            
            val = val.replaceAll("\"|\'|,|-|~|;|\\(|\\)|\\[|]|/|_|\\\\|\\$|#|@|%|\\*|(\\.){2,}", "");
            val = val.replaceAll("\\\\u.{1,4}", "");
            val = val.toLowerCase();
            
            for (int i = 0; i < val.length(); ++i) {
                char c = val.charAt(i);
                if (i < val.length()-1 && c == '.' && Character.isLetterOrDigit(val.charAt(i+1)))
                    val = new StringBuilder(val).deleteCharAt(i).toString();
                else if (!Character.isLetterOrDigit(c) && c != '.' && c != ' ')
                    val = new StringBuilder(val).deleteCharAt(i).toString();
            }
    
            val = val.replaceAll(" +", " ");
            val = val.trim();
            
            if (val.isEmpty())
                return;
            
            String[] sentences = val.split("\\.");
            for (String s : sentences) {
                s = s.trim();
                if (!s.isEmpty()) {
                    String[] words = s.split(" ");
                    context.write(new Text("<s> "+ words[0]), one);
                    for (int i = 0; i < words.length-1; ++i) {
                        context.write(new Text(words[i] +" "+ words[i+1]), one);
                    }
                    context.write(new Text(words[words.length-1]+" </s>"), one);
                }
            }
        }
        
    }
    
    public static class Reducer1 extends Reducer<Text, IntWritable, Text, Text> {
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            
            Integer sum = 0;
            
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            context.write(key, new Text(sum.toString()));
        }
        
    }

    public static void main(String[] args) {
        
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Job 1: Cleaning joined dataset");
            job.setNumReduceTasks(8);
            job.setJarByClass(TP2_FrequencyCollection.class);
            job.setMapperClass(Mapper1.class);
            job.setReducerClass(Reducer1.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path("CleanedJoinedDataset"));
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
