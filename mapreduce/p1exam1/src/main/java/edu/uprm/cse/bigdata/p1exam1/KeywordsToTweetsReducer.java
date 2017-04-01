package edu.uprm.cse.bigdata.p1exam1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KeywordsToTweetsReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String output = "";

        for (Text value : values){
            output += value.toString() + ", ";
        }
        context.write(key, new IntWritable(output.substring(0, output.length()-2)));
    }
}