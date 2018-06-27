package rediscluster

import (
	"log"
	"time"
	"fmt"
	"strings"
	"github.com/gin-blog/pkg/setting"	
	"github.com/chasex/redis-go-cluster"
)
var cluster *redis.Cluster
//初始化redis链接
func init(){
	var (
		hosts string
		err error
	)
	sources, err := setting.Cfg.GetSection("redis-cluster")
	if err != nil {
		log.Fatal(2, "Fail to get section 'redis-cluster': %v", err)
	}
	hosts = sources.Key("HOST").String()
	host_map := strings.Split(hosts,",")
	cluster, err = redis.NewCluster(
	    &redis.Options{
		StartNodes: host_map,
		ConnTimeout: time.Duration(sources.Key("CONNECT_TIME").MustInt(60)) * time.Millisecond,
		ReadTimeout: time.Duration(sources.Key("READ_TIMEOUT").MustInt(60)) * time.Millisecond,
		WriteTimeout: time.Duration(sources.Key("READ_TIMEOUT").MustInt(60)) * time.Millisecond,
		KeepAlive: 16,
		AliveTime: 60 * time.Second,
	    })
	if err != nil {
		log.Fatal(2, "Fail to connect 'redis-cluster': %v", err)
	}
//	list := map[interface{}]interface{}{"aa":"a","cc":"2","bb":"c","tt":"c"}
//	reply :=Mset(list)
//	reply,err := Mget([]interface{}{"aa","","cc"})
	err = Hmset("aaa",map[interface{}]interface{}{"aa":2,"bb":3,"cc":4})
//log.Fatal(2, "res': %v", err)
//	temp := []interface{}{"hash","f11","1","f22","2"}
//	_,err = cluster.Do("HMSET", temp...)
	reply,err := Hgetall("aaa")
	log.Fatal(2, "res': %v", reply)
}

func Set(key string,value string)(res bool,err error){
	reply,err := cluster.Do("SET", key, value)
	if reply=="OK" {
		return true,nil
	}
	return false,err
}

func Get(key string)(reply string,err error){
	reply, err = redis.String(cluster.Do("GET", key))
	return
}

func Incr(key string,step int)(reply int,err error){
	reply, err = redis.Int(cluster.Do("INCR", key, step))
	return
}

func Hgetall(key string)(reply  map[string]string,err error){
	reply, err = redis.StringMap(cluster.Do("HGETALL",key+"_hash"))
	return
}

//err = batch.Put("INCRBY", "countries", 3)
//list为键值对字典,对应的获取见Lrange
func Lpush(key string,lists []string)(reply []interface {},err error){
	batch := cluster.NewBatch()
	for _,value := range lists{
		err = batch.Put("LPUSH", key, value)
	}
	reply, err = cluster.RunBatch(batch)
	return
}

func Lrange(key string)(reply []string,err error){
	reply, err = redis.Strings(cluster.Do("LRANGE", key, 0, -1))
	return
}

func Mset(list map[interface{}]interface{}) bool{
	temp := MapToSlice(list,"")
	_, err := cluster.Do("MSET", temp...)	
	if err != nil{
		return false
	}
	return true
}
//只返回keys对应的结果，key不再返回，有点蛋疼
func Mget(keys []interface {})(values []string,err error){
	values, err = redis.Strings(cluster.Do("MGET", keys...))
	return
}

//map转换为切片
func MapToSlice(input map[interface{}]interface{},hash_key string) []interface{} {
    output := []interface{}{}
	if hash_key!= ""{
		output = append(output,hash_key)
	}
    for key,value:=range input {
		output = append(output,key)
        output = append(output,value)
    }
    return output
}

func Hmset(hash_key string,list map[interface{}]interface{})error{
	temp := MapToSlice(list,hash_key+"_hash")
	_,err := cluster.Do("HMSET", temp...)
	return err
}


func typeof(v interface{}) string {
    return fmt.Sprintf("%T", v)
}

// 表示可以传任意值
func Print(args ...interface{}) {
	for _, res := range args {
		switch res.(type) {
			case int :
				fmt.Println(res, " is int")
			case float64 :
				fmt.Println(res, " is float64");
			case string :
				fmt.Println(res, " is string")
		}
	}
}
