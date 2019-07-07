# HBase Introduction in Depth

This is complementary source codes for [HBase徹底入門](https://www.amazon.co.jp/dp/B00SV4V34E/ref=cm_sw_em_r_mt_dp_U_NPDiDb4Y68MNV).
The original sources are based on HBase 0.98.6 (CDH 5.2.1) which is a bit old as of today, so the ones under this repo are updated for HBase 2.0.5.
Also, these are completely rewritten as testable codes (with JUnit5), so you can build/test these sources on your own laptop with the following complementary [HBase Docker container](https://github.com/tsuyo/apache-hbase-docker).

```
$ sudo vi /etc/hosts
...
127.0.0.1	localhost docker-host # need this one for zookeeper + docker issue
...
$ docker run -h docker-host --name apache-hbase-pseudo --rm -d \
-p 50070:50070 -p 9000:9000 \
-p 50010:50010 -p 50075:50075 -p 50020:50020 \
-p 8031:8031 -p 8030:8030 -p 8032:8032 -p 8088:8088 -p 8033:8033 \
-p 8040:8040 -p 13562:13562 -p 8042:8042 \
-p 10020:10020 -p 19888:19888 \
-p 50100-50105:50100-50105 \
-p 2181:2181 \
-p 16000:16000 -p 16010:16010 \
-p 16020:16020 -p 16030:16030 \
-v ${PWD}:/workspace kirasoa/apache-hbase-pseudo:2.0.5
$ git clone https://github.com/tsuyo/hbase-introduction-in-depth
$ cd hbase-introduction-in-depth
$ mvn -DskipTests clean package # build a package first for MapReduce jobs
$ mvn -DHADOOP_USER_NAME=hadoop test # all tests should pass
```
