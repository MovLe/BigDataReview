package Demo04FlowSort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName FlowSortReducer
 * @MethodDesc: 用户流量排序Reducer
 * @Author Movle
 * @Date 11/10/20 3:00 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class FlowSortReducer extends Reducer<FlowBean, Text,Text,FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value:values) {
            context.write(value,key);
        }
    }
}
