import java.io.*;
import java.util.*;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.filecache.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Joiner {
    public static class ClassMapper extends Mapper<Object, Text, Text, Text> {
    
        String getFieldValue(String line, String field) {
            String fieldStr = "\"" + field + "\":";
            int start = line.indexOf(fieldStr);
            //Up to the start of the value
            String cutLine = line.substring(start + fieldStr.length() + 2);
            int end = cutLine.indexOf('\"');
            
            return cutLine.substring(0, end);
        }
    
        private Text ID = new Text();
        private Text IDClass = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString();
            
            ID = new Text(getFieldValue(val, "id"));
            IDClass = new Text("C" + getFieldValue(val, "truthClass"));
            
            context.write(ID, IDClass);
        }
    }
    public static class TitleMapper extends Mapper<Object, Text, Text, Text> {

        String getFieldValue(String line, String field, char delim) {
            String fieldStr = "\"" + field + "\":";
            int start = line.indexOf(fieldStr);
            //Up to the start of the value
            String cutLine = line.substring(start + fieldStr.length() + 2);
            int end = cutLine.indexOf(delim);
            
            return cutLine.substring(0, end);
        }
    
        private Text ID = new Text();
        private Text IDTitle = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //Chop the useless postmedia field off
            String line = value.toString();

            ID = new Text(getFieldValue(line, "id", '"'));
            IDTitle = new Text("H" + getFieldValue(line, "postText", ']'));
            
            context.write(ID, IDTitle);
        }
    }

    public static class ClickBaitReducer extends Reducer<Text, Text, Text, Text> {
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Boolean isClickbait = false;
            Text Title = new Text();
            
            for (Text t : values) {
                String val = t.toString();
                char type = val.charAt(0);
                String valTxt = val.substring(1);
                
                if (type == 'C' && valTxt.equals("clickbait")) 
                    isClickbait = true;
                else if (type == 'H') {
                    //valTxt = valTxt.substring(1, valTxt.length() - 2);
                    Title = new Text(valTxt);
                }
            }
            
            if (isClickbait) context.write(key, Title);
        }
    }
    
    //Map the examiner headlines
    public static class EXMapper extends Mapper<Object, Text, NullWritable, Text> {

        private Text headline = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");        
            if (parts.length < 2 || parts[0].equals("publish_date")) return;
            
            headline = new Text(parts[1]);
            context.write(NullWritable.get(), headline);
        }
    }
    
    //Map the Click Bait Challenge headlines
    public static class CBCMapper extends Mapper<Object, Text, NullWritable, Text> {

        private Text headline = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
            String[] parts = value.toString().split("\t");
            
            headline = new Text(parts[1]);
            context.write(NullWritable.get(), headline);
        }
    }
    
    public static class HeadlineReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t : values) {
                String v = t.toString();
                //Must atleast be ""
                if (v.length() > 1) {
                    //Slice off the leading and trailing "
                    v = v.substring(1, v.length() - 1);
                    context.write(NullWritable.get(), new Text(v));
                }
            }
            
            
        }
    }
    
    public static void DoJob(Configuration conf, Path in1, Path in2, Path out1, Path in3, Path out2) throws Exception {
        Job ccFilter = Job.getInstance(conf, "Clickbait filter");
        
        //instances.jsonl
        MultipleInputs.addInputPath(ccFilter, in1, TextInputFormat.class, TitleMapper.class);
        //truth.jsonl
        MultipleInputs.addInputPath(ccFilter, in2, TextInputFormat.class, ClassMapper.class);
        
        ccFilter.setReducerClass(ClickBaitReducer.class);
        //Write ID  Title
        FileOutputFormat.setOutputPath(ccFilter, out1);
    
        ccFilter.setJarByClass(Joiner.class);
        
        ccFilter.setMapOutputKeyClass(Text.class);
        ccFilter.setMapOutputValueClass(Text.class);
        
        System.out.println("Filtering out non-clickbait headlines");
        
        if (ccFilter.waitForCompletion(true)) {
    
            Job joinerJob = Job.getInstance(conf, "Headline Join");

            //The already filtered clickbait headlines
            MultipleInputs.addInputPath(joinerJob, out1, TextInputFormat.class, CBCMapper.class);
            //The examiner headlines
            MultipleInputs.addInputPath(joinerJob, in3, TextInputFormat.class, EXMapper.class);
            
            joinerJob.setReducerClass(HeadlineReducer.class);
            joinerJob.setNumReduceTasks(1);
            //Write ID  Title
            FileOutputFormat.setOutputPath(joinerJob, out2);
        
            joinerJob.setJarByClass(Joiner.class);
            
            joinerJob.setMapOutputKeyClass(NullWritable.class);
            joinerJob.setMapOutputValueClass(Text.class);
            
            System.out.println("Joining article headlines");
            
            joinerJob.waitForCompletion(true);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //          instances.jsonl            truth.jsonl            Clickbait filtered output location 
        //          The examiner data          Joined output location
        DoJob(conf, new Path("/TP/instances"), new Path("/TP/truth"), new Path("/TP/cbc-titles"), new Path("/TP/examiner"), new Path("/TP/clickbait-headlines"));
   } 
    
}
