package Demo01WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName WordCountMapper
 * @MethodDesc: WordCount的Mapper
 * @Author Movle
 * @Date 11/9/20 9:19 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");

        for (String word:words) {
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
