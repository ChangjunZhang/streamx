import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.entity.StringEntity;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

public class FeiShuTest {
    public static String doPost(String url, JsonNode json) {
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost httpPost = new HttpPost(url);
        //api_gateway_auth_token自定义header头，用于token验证使用
        httpPost.addHeader("Content-Type", "application/json;charset=utf-8");
        httpPost.addHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.81 Safari/537.36");
        try {
            StringEntity se = new StringEntity(json.toString());
            se.setContentEncoding("UTF-8");
            //发送json数据需要设置contentType
            se.setContentType("application/x-www-form-urlencoded");
            //设置请求参数
            httpPost.setEntity(se);
            HttpResponse response = httpClient.execute(httpPost);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                //返回json格式
                String res = EntityUtils.toString(response.getEntity());
                return res;
            }
            return String.valueOf(response.getStatusLine().getStatusCode());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (httpClient != null){
                try {
                    httpClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }
    @Test
    public void testSendMsg (){
        Map<String,Object> header  = new HashMap<>();
        header.put("Content-Type","application/json");
        JsonMapper jsonMapper = JsonMapper.builder().build();
        ObjectNode body = jsonMapper.createObjectNode();
        body.put("msg_type","text");
        ObjectNode content = jsonMapper.createObjectNode();
        content.put("text","request example");
        body.set("content",content);
        String feishuWebHook="https://open.feishu.cn/open-apis/bot/v2/hook/5c857264-e5b6-44ac-8845-0fcaadd4a529";

        String result = doPost(feishuWebHook, body);
        System.out.println(result);

    }
}
