package Demo05Partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName FlowCountReducer
 * @MethodDesc: 统计流量的Reducer
 * @Author Movle
 * @Date 11/10/20 2:09 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class FlowCountReducer extends Reducer<Text, FlowBean,Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long sum_upFlow =0;
        long sum_downFlow=0;

        for (FlowBean bean:values) {
            sum_upFlow += bean.getUpFlow();
            sum_downFlow += bean.getDownFlow();
        }

        FlowBean resultBean = new FlowBean(sum_upFlow,sum_downFlow);

        context.write(key,resultBean);
    }
}
