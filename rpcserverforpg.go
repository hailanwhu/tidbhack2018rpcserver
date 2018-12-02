package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strings"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "hailan"
	password = ""
	dbname   = "hailan"
)


type RequestPG struct{
	SQL string
	Count int
}

type ResultPG struct{
	Result string
}

type PGX struct{

}


func (c *PGX)Require(args *RequestPG,reply *ResultPG)error{

	log.Println(args.SQL)

	reply.Result = query(args.SQL,args.Count)
	return nil
}

func query(sqlStmt string,columnCount int)string{
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}


	log.Println(columnCount)
	sqlStatement := sqlStmt
	row,_:=db.Query(sqlStatement)
	result := ""
	count :=0
	for row.Next(){
		if count>0 {
			result+=","
		}
		var resTemp = make([]interface{},columnCount)
		var res = make([]string,columnCount)
		for i:=0;i<columnCount ;i++  {
			resTemp[i]=&res[i]
		}
		for i:=0;i< len(res);i++  {
			row.Scan(resTemp...)
		}
		tempS:=strings.Join(res,"#")
		result+=tempS
		log.Println(res)
		count++
	}
	return result
}

func main(){

	pgx := new(PGX)
	rpc.Register(pgx)
	rpc.HandleHTTP()
	l,e:=net.Listen("tcp","0.0.0.0:5434")
	if e!=nil {
		log.Println(e)
	}
	http.Serve(l,nil)

	//log.Println(query("select a3 from test",1))
}