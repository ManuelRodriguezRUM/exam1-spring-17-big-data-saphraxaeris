package edu.uprm.cse.bigdata.p1exam1;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class App 
{
    public static void main( String[] args )
    {
        if (args.length != 2) {
            System.err.println("Usage: <input path> <output directory>");
            System.exit(-1);
        }
        try {
            Job job = Job.getInstance();
            job.setJarByClass(edu.uprm.cse.bigdata.p1exam1.App.class);
            job.setJobName("Exam 1");

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.setMapperClass(edu.uprm.cse.bigdata.p1exam1.KeywordsToTweetsMapper.class);
            job.setReducerClass(edu.uprm.cse.bigdata.p1exam1.KeywordsToTweetsReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.waitForCompletion(true);
        }
        catch(Exception ex) {
            System.err.println(ex.getMessage());
            System.exit(-1);
        }
    }
}
