package main

import (
	"bufio"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
)


type Requestx struct{
	Start int
	TableName string
	Offsets []int
}

type Resultx struct{
	Count int
	Result string
}

type CSVX struct{

}


func readFile(path string,offset []int)(int,string){
	f,err := os.OpenFile("/Users/Hai/Desktop/"+path,os.O_RDONLY,0644)
	if err != nil{
		log.Println("OPEN ERROR")
		return 0,"" // there maybe some send some infos to let the csvSR know nothing this time
	}
	defer f.Close()
	var count = 0
	var stringInfo = ""
	reader := bufio.NewReader(f)
	for{
		recordTemp,_,err:=reader.ReadLine()
		if err!=nil {
			//log.Println("Read Over")
			break
		}
		count++
		log.Println(count)
		if count>1 {
			stringInfo+=","
		}
		//dataVal := make([]interface{},0)
		recordTemp = recordTemp[:len(recordTemp)]

		recordTempS := string(recordTemp)
		record := strings.Split(recordTempS,",")
		tempS := ""
		for i:=0;i<len(offset) ;i++  {
			if i>0 {
				tempS+="#"
			}
			tempS+=record[offset[i]]
		}
		stringInfo+=tempS
	}
	return count,stringInfo
}



func (c *CSVX)Require(args *Requestx,reply *Resultx)error{
	log.Println(args.Start)
	log.Println(args.TableName)
	log.Println(args.Offsets)
	//reply.Count=10
	if args.Start==11 {
		reply.Count=12
	}
	count,info := readFile(args.TableName,args.Offsets)
	reply.Count=count
	reply.Result=info
	return nil
}

func main(){
	csvx := new(CSVX)
	rpc.Register(csvx)
	rpc.HandleHTTP()
	l,e:=net.Listen("tcp","0.0.0.0:5433")
	if e!=nil {
		log.Println(e)
	}
	http.Serve(l,nil)


}

