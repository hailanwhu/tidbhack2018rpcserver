package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type RequestRedis struct{
	/*
	1)support key match:point and RE[1,2]
	2)value:point [3]
	3)the first version is just string forms
	4)no condition read all k{v}[4]
	*/
	QueryType int
	Offsets []int //0 for key and 1 for value
	Value string //this is prama eg:a="b" b is para
	Count int
}

type ResultRedis struct{
	Result string //the same as pg and csv
}

type RedisXX struct{

}


func (c *RedisXX)Require(args *RequestRedis,reply *ResultRedis)error{
	v:=""
	log.Println(args.QueryType)
	log.Println(args)
	switch args.QueryType {
		case 1:
			v=queryRedisPointQueryForKey(args.Offsets,args.Value)
		case 2:
			v=queryRedisREForKey(args.Offsets,args.Value)
		case 4:
			v=queryRedisNoCondtion(args.Offsets)
	}
	reply.Result=v
	log.Println(v)
	return nil
}

func queryRedisPointQueryForKey(offset []int,para string)string{
	//连接redis
	c, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Println(err)
		return ""
	}
	defer c.Close()

	//通过Do函数，发送redis命令
	//redis.
	v, err := redis.String(c.Do("GET", para))
	if err != nil {
		fmt.Println(err) //this mean no result
		return ""
	}

		if len(offset)==1 {
			//
			if offset[0]==0 {
				return para
			}
			if offset[0]==1{
				return v
			}
		}else{
			if offset[0]==0 {
				return para+"#"+v
			}else{
				return v+"#"+para
			}
		}
	return v
}

func queryRedisREForKey(offset []int,para string)string{
	c, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Println(err)
		return ""
	}
	defer c.Close()
	keys, err := redis.Strings(c.Do("KEYS", para))
	v := ""
	if err != nil {
		return v
	}
	for i, key := range keys {
		//fmt.Println(key)
		if i>0 {
			v+=","
		}
		value, _ := redis.String(c.Do("GET", key)) //must exsit

		//compose one reuslt
		oneResult :=""

		if len(offset)==1 {
			if offset[0]==0 {
				oneResult+=key
			}
			if offset[0]==1{
				oneResult+=value
			}
		}else{
			if offset[0]==0 {
				oneResult= key+"#"+value
			}else{
				oneResult =  value+"#"+key
			}
		}
		v+=oneResult

	}
	return v
}

func queryRedisNoCondtion(offset []int)string{
	c, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Println(err)
		return ""
	}
	defer c.Close()
	keys, err := redis.Strings(c.Do("KEYS", "*"))
	v := ""
	if err != nil {
		return v
	}
	for i, key := range keys {
		//fmt.Println(key)
		if i>0 {
			v+=","
		}
		value, _ := redis.String(c.Do("GET", key)) //must exsit

		//compose one reuslt
		oneResult :=""

		if len(offset)==1 {
			if offset[0]==0 {
				oneResult+=key
			}
			if offset[0]==1{
				oneResult+=value
			}
		}else{
			if offset[0]==0 {
				oneResult= key+"#"+value
			}else{
				oneResult =  value+"#"+key
			}
		}
		v+=oneResult
	}
	return v
}

func testget(){
	c, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c.Close()

	//通过Do函数，发送redis命令
	//redis.
	v, err := redis.String(c.Do("GET", "lanhaihailanlanhai"))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(v)
}

func main(){
	rxx := new(RedisXX)
	rpc.Register(rxx)
	rpc.HandleHTTP()
	l,e:=net.Listen("tcp","0.0.0.0:5435")
	if e!=nil {
		log.Println(e)
	}
	http.Serve(l,nil)
}
