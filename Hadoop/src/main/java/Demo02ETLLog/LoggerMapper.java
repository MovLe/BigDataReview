package Demo02ETLLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName LoggerMapper
 * @MethodDesc: Mapper
 * @Author Movle
 * @Date 11/10/20 11:25 上午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class LoggerMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        LogBean bean = parseLog(line);

        if(!bean.isVaild()){
            return;
        }
        k.set(bean.toString());

        context.write(k,NullWritable.get());
    }

    private LogBean parseLog(String line) {
        LogBean logBean = new LogBean();
        String[] field = line.split(" ");
        if(field.length>11){
            logBean.setRemote_addr(field[0]);
            logBean.setRemote_user(field[1]);
            logBean.setTime_local(field[3].substring(1));
            logBean.setRequest(field[6]);
            logBean.setStatus(field[8]);
            logBean.setBody_bytes_sent(field[9]);
            logBean.setHttp_referer(field[10]);

            if(field.length>12){
                logBean.setHttp_user_agent(field[11]+" "+field[12]);
            }else {
                logBean.setHttp_user_agent(field[11]);
            }


            if(Integer.parseInt(logBean.getStatus())>=400){
                logBean.setVaild(false);
            }
        }else {
            logBean.setVaild(false);
        }

        return logBean;

    }
}
