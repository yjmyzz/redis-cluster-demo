package com.cnblogs.yjmyzz.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Map;
import java.util.Set;


public class AppDemo {

    private static Logger logger = LoggerFactory.getLogger(AppDemo.class);
    private static JedisCluster jc = null;

    private static final String KEYS_STRING = "STRING";
    private static final String KEYS_SET = "SET";
    private static final String KEYS_LIST = "LIST";
    private static final String KEYS_HASH = "HASH";
    private static final String KEYS_ZSET = "ZSET";


    private static void addKey(final String conainter, final String key) {
        if (!jc.exists(conainter)) {
            jc.sadd(conainter, key);
        } else {
            if (!jc.smembers(conainter).contains(key)) {
                jc.sadd(conainter, key);
            }
        }
    }

    /**
     * 写入字符串缓存
     *
     * @param key
     * @param value
     * @return
     */
    private static String set(final String key, final String value) {
        String result = jc.set(key, value);
        addKey(KEYS_STRING, key);
        return result;
    }

    /**
     * 写入Set缓存
     *
     * @param key
     * @param member
     * @return
     */
    private static Long sadd(final String key, final String... member) {
        Long result = jc.sadd(key, member);
        addKey(KEYS_SET, key);
        return result;
    }


    /**
     * 从左侧写入List
     *
     * @param key
     * @param string
     * @return
     */
    private static Long lpush(final String key, final String... string) {
        Long result = jc.lpush(key, string);
        addKey(KEYS_LIST, key);
        return result;
    }


    /**
     * 写入HashMap缓存
     *
     * @param key
     * @param field
     * @param value
     * @return
     */
    private static Long hset(final String key, final String field, final String value) {
        Long result = jc.hset(key, field, value);
        addKey(KEYS_HASH, key);
        return result;
    }


    /**
     * 写入ZSet缓存
     *
     * @param key
     * @param score
     * @param member
     * @return
     */
    private static Long zadd(final String key, final double score, final String member) {
        Long result = jc.zadd(key, score, member);
        addKey(KEYS_ZSET, key);
        return result;
    }

    private static Long zadd(final String key, final String member) {
        Long result = jc.zadd(key, 0d, member);
        addKey(KEYS_ZSET, key);
        return result;
    }


    public static void main(String[] args) {

        ApplicationContext ctx = new ClassPathXmlApplicationContext("spring-redis.xml");

        jc = ctx.getBean(JedisCluster.class);


        Map<String, JedisPool> nodes = jc.getClusterNodes();
        for (Map.Entry<String, JedisPool> entry : nodes.entrySet()) {
            logger.info(entry.getKey() + " => " + entry.getValue().toString());
            //清空所有数据
            try {
                entry.getValue().getResource().flushDB();
            } catch (Exception e) {
                logger.info(e.getLocalizedMessage());//slave节点上执行flushDB会报错
            }
            //entry.getValue().getResource().keys("*");//慎用,缓存数量较大时,会引起性能问题.
        }

        //检测key是否存在
        logger.info(jc.exists("a").toString());

        //字符串写入测试
        logger.info(set("a", "hello world!"));
        logger.info(set("b", "hello redis!"));

        //字符串读取测试
        logger.info(jc.get("a"));

        //set写入操作
        logger.info("set写入测试 ==>");
        logger.info(sadd("set1", "a", "b", "c") + "");

        //缓存类型测试
        logger.info(jc.type("set1"));

        //set读取测试
        logger.info("set读取测试 ==>");
        Set<String> set1 = jc.smembers("set1");
        for (String s : set1) {
            logger.info(s);
        }

        //list写入测试
        logger.info("list写入测试 ==>");
        logger.info(lpush("list1", "1", "2", "3") + "");


        //list读取测试
        logger.info("list读取测试 ==>");
        List<String> list1 = jc.lrange("list1", 0, 999);
        for (String s : list1) {
            logger.info(s);
        }

        //hash写入测试
        logger.info("hash写入测试 ==>");
        logger.info(hset("hash1", "jimmy", "杨俊明") + "");
        logger.info(hset("hash1", "CN", "中国") + "");
        logger.info(hset("hash1", "US", "美国") + "");

        //hash读取测试
        logger.info("hash读取测试 ==>");
        Map<String, String> hash1 = jc.hgetAll("hash1");
        for (Map.Entry<String, String> entry : hash1.entrySet()) {
            logger.info(entry.getKey() + ":" + entry.getValue());
        }

        //zset写入测试
        logger.info("zset写入测试 ==>");
        logger.info(zadd("zset1", "3") + "");
        logger.info(zadd("zset1", "2") + "");
        logger.info(zadd("zset1", "1") + "");
        logger.info(zadd("zset1", "4") + "");
        logger.info(zadd("zset1", "5") + "");
        logger.info(zadd("zset1", "6") + "");

        //zset读取测试
        logger.info("zset读取测试 ==>");
        Set<String> zset1 = jc.zrange("zset1", 0, 999);
        for (String s : zset1) {
            logger.info(s);
        }

        //遍历所有缓存项的key
        logger.info("遍历cluster中的所有key ==>");
        logger.info(jc.smembers(KEYS_STRING).toString());
        logger.info(jc.smembers(KEYS_HASH).toString());
        logger.info(jc.smembers(KEYS_SET).toString());
        logger.info(jc.smembers(KEYS_LIST).toString());
        logger.info(jc.smembers(KEYS_ZSET).toString());

    }
}
