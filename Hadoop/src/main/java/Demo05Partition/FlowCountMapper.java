package Demo05Partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName FlowCountMapper
 * @MethodDesc: 统计手机流量的Mapper
 * @Author Movle
 * @Date 11/10/20 2:02 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class FlowCountMapper extends Mapper<LongWritable, Text,Text, FlowBean> {

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String line = value.toString();
        String[] fields = line.split("\t");

        String phoneNum = fields[1];
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        k.set(phoneNum);


        context.write(k,new FlowBean(upFlow,downFlow));

    }
}
