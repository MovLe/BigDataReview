package Demo01WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName WordCountReducer
 * @MethodDesc: 第一个WordCount的Reducer
 * @Author Movle
 * @Date 11/9/20 9:25 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count =0;

        for (IntWritable value:values) {
            count += value.get();
        }

        context.write(key,new IntWritable(count));
    }
}
