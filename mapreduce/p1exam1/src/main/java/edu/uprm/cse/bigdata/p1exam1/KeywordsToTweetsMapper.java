package edu.uprm.cse.bigdata.p1exam1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

public class KeywordsToTweetsMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String[] tweetWords = status.getText().toLowerCase().replace("#", "").split(" ");

            for(String word : tweetWords) {
                if(word.equals("trump"))
                    context.write(new Text("Trump"), new Text(status.getId() + ""));
                else if(word.equals("maga"))
                    context.write(new Text("MAGA"), new Text(status.getId() + ""));
                else if(word.equals("dictator"))
                    context.write(new Text("Dictator"), new Text(status.getId() + ""));
                else if(word.equals("impeach"))
                    context.write(new Text("Impeach"), new Text(status.getId() + ""));
                else if(word.equals("drain"))
                    context.write(new Text("Drain"), new Text(status.getId() + ""));
                else if(word.equals("swamp"))
                    context.write(new Text("Swamp"), new Text(status.getId() + ""));
                else if(word.equals("change"))
                    context.write(new Text("Change"), new Text(status.getId() + ""));
            }
        }
        catch(TwitterException e) { /* Do nothing */ }
    }
}