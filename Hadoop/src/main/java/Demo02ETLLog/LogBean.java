package Demo02ETLLog;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @ClassName LogBean
 * @MethodDesc: 封装日志数据
 * @Author Movle
 * @Date 11/10/20 10:44 上午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class LogBean {
    private String remote_addr;
    private String remote_user;
    private String time_local;
    private String request;
    private String status;
    private String body_bytes_sent;
    private String http_referer;
    private String http_user_agent;

    private boolean vaild = true;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.vaild);
        sb.append("\001").append(this.remote_addr);
        sb.append("\001").append(this.remote_user);
        sb.append("\001").append(this.time_local);
        sb.append("\001").append(this.request);
        sb.append("\001").append(this.status);
        sb.append("\001").append(this.body_bytes_sent);
        sb.append("\001").append(this.http_referer);
        sb.append("\001").append(this.http_user_agent);


        return sb.toString();
    }
}
