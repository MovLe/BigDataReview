package Demo01WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName WordCountPartioner
 * @MethodDesc: WordCount的Partioner方法
 * @Author Movle
 * @Date 11/9/20 10:21 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class WordCountPartioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        String line = text.toString();

        int num = intWritable.get();
        if(num%2==0){
            return 0;
        }else {
            return 1;
        }
    }
}
