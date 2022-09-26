package chronocache.db;

import chronocache.core.qry.QueryResult;
import com.google.gson.Gson;
import redis.clients.jedis.JedisPooled;
import chronocache.util.Configuration;

import java.util.List;
import java.util.Map;

public class RedisClient implements QueryResultCache {
    private JedisPooled pooled;
    private Gson gson;

    public RedisClient() {
        this.pooled = new JedisPooled(Configuration.getRedisAddress(), Configuration.getRedisPort());
        this.gson = new Gson();
    }

    @Override
    public QueryResult get(String key) {
        String result = pooled.get(key);
        if (result == null) {
            return null;
        }
        List<Map<String, Object>> selectResult = gson.fromJson(result, List.class);
        return new QueryResult(selectResult, null);
    }

    @Override
    public void put(String key, QueryResult result) {
        String json = gson.toJson(result.getSelectResult());
        pooled.set(key, json);
    }
}
