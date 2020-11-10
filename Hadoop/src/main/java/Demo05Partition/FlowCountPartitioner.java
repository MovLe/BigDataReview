package Demo05Partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName FlowCountPartitioner
 * @MethodDesc: 分区
 * @Author Movle
 * @Date 11/10/20 3:27 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class FlowCountPartitioner extends Partitioner<Text,FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String preNum = text.toString().substring(0,3);

        int partition =4;

        if("136".equals(preNum)){
            partition=0;
        }else if("137".equals(preNum)){
            partition=1;
        }else if("138".equals(preNum)){
            partition=2;

        }else {
            partition=3;
        }
        return partition;
    }
}
