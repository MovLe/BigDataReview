package Demo01WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName WordCountDriver02
 * @MethodDesc: 带分区的Driver
 * @Author Movle
 * @Date 11/9/20 10:16 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class WordCountDriver02 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args =new String[] {"/Users/macbook/TestInfo2/a.txt","/Users/macbook/TestInfo2/WC"};

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountDriver02.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setPartitionerClass(WordCountPartioner.class);
        job.setNumReduceTasks(2);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);

    }
}
