package main

import (
	"encoding/json"
	"math"
	//"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	//"os"
	"strconv"
	//"strings"
	//"sync"
	//"sync/atomic"
	//
	//"math"
	//"net"
	//"time"


	//"os"

	model "model"

)



var globalDeployServerNum int32
var globalConnectServerNum int32
var globalDoneServerNum  int32

//var AllPortMap map[string]int
//var AllIpMap map[string]string
//var globalPeerInfo map[string]map[string]map[string]int
//var globalSendPeerMap map[string][]string



var AllPortMap sync.Map
var AllIpMap sync.Map
var globalPeerInfo  sync.Map
var globalSendPeerMap sync.Map





func GetPortAndIpMap()(AllPortMap ,AllIpMap,globalPeerInfo,globalSendPeerMap sync.Map){

	sql := `SELECT * FROM as_distribution_info`
	var As_distribution_infos []model.As_distribution_info
	err := model.XormEngine.SQL(sql).Find(&As_distribution_infos)
	if err != nil{
		fmt.Println("get As_distribution_infos err:",err)
	}
	for _,value := range As_distribution_infos{
		var as_peer []model.As_peer
		err:=json.Unmarshal([]byte(value.Peer),&as_peer)
		if err != nil{
			fmt.Println(err)
		}
		//peer_map := make(map[string]map[string]int)
		var peer_map sync.Map
		var globalSendPeerList []string
		for _,peer_value := range as_peer{
			peer_in_out_map := make(map[string]int)
			peer_in_out_map["in"] = peer_value.In
			peer_in_out_map["out"] = peer_value.Out
			peer_map.Store(peer_value.Name,peer_in_out_map)
			peerNum, _ := strconv.Atoi(peer_value.Name[2:])
			selfNum, _ := strconv.Atoi(value.Name[2:])
			if peerNum > selfNum{
				globalSendPeerList = append(globalSendPeerList, peer_value.Name)
			}
		}
		globalSendPeerMap.Store(value.Name,globalSendPeerList)
		globalPeerInfo.Store(value.Name,peer_map)
		AllPortMap.Store(value.Name,value.Tcp_port)
		AllIpMap.Store(value.Name,value.Ip)
	}
	return
}



func distribution()(As_distribution_infos []model.As_distribution_info,as_distribution_done model.As_distribution_done){
	ip, err := model.ExternalIP()
	if err != nil{
		fmt.Println("getIP err:",err)
	}
	name, err := os.Hostname()
	if err != nil{
		fmt.Println("getIP err:",err)
	}
	fmt.Println("ip：",ip.String(),"name,",name)


	session := model.XormEngine.NewSession()
	if err := session.Begin(); err != nil {
		session.Rollback()
		fmt.Println(session, "session.Begin() fail", err)
	}
	//sqlStr := `Update as_distribution_done (container_name,ip) VALUES  (?)`
	//res, err := session.Exec(sqlStr, "zhang")

	//得到所有记录的长度
	var asLength int
	sql := `select count(id) as asLength from as_distribution_info `
	_, err = model.XormEngine.SQL(sql).Get(&asLength)
	if err != nil{
		fmt.Println("getAllLength err:",err)
	}
	fmt.Println("asLength,",asLength)
	//得到容器个数
	var containerNum int
	sql = `select count(id) as containerNum from as_distribution_done `
	_, err = model.XormEngine.SQL(sql).Get(&containerNum)
	if err != nil{
		fmt.Println("get container Length err:",err)
	}
	fmt.Println("containerNum:,",containerNum)
	//得到自己的序号
	as_distribution_done = model.As_distribution_done{}
	sql = `select * from as_distribution_done `
	_, err = model.XormEngine.Table("as_distribution_done").Where(" container_name = ?",name).Get(&as_distribution_done)
	if err != nil{
		fmt.Println("get As_distribution_done err:",err)
	}
	fmt.Println("as_distribution_done,",as_distribution_done)
	//计算分区
	perCount := int(math.Ceil(float64(asLength)/float64(containerNum)))
	startIndex := perCount*(as_distribution_done.Container_num)
	sql = `SELECT * FROM as_distribution_info order by id asc LIMIT ?,?`
	fmt.Println("sql:",sql,"startindex",startIndex,"perCount",perCount)
	err = model.XormEngine.SQL(sql,startIndex,perCount).Find(&As_distribution_infos)
	if err != nil{
		fmt.Println("get As_distribution_infos err:",err)
	}
	fmt.Println("计算得到分区，分区数量",len(As_distribution_infos))
	freePort,err := model.GetFreePorts(len(As_distribution_infos))
	fmt.Println("计算得到分区，端口数量",len(freePort))
	for index,_ := range As_distribution_infos{
		As_distribution_infos[index].Tcp_port = freePort[index]
		As_distribution_infos[index].Ip = ip.String()
		As_distribution_infos[index].Container_name = name

		//fmt.Println("更新,",As_distribution_infos[index])
		model.XormEngine.Table("as_distribution_info").ID(As_distribution_infos[index].Id).Update(&As_distribution_infos[index])
	}


	fmt.Println("分区ip端口更新完毕")
	as_distribution_done.Distribution_done = 1
	as_distribution_done.Ip = ip.String()

	model.XormEngine.Table("as_distribution_done").ID(as_distribution_done.Id).Update(&as_distribution_done)
	fmt.Println(as_distribution_done)

	err = session.Commit()
	if err != nil{
		fmt.Println("commit err:",err)
	}
	return
}




func main() {
	model.InitMysql()
	defer model.XormEngine.Close()

	As_distribution_infos ,as_distribution_done:= distribution()


	for {
		var undoneCount int
		sql := `select count(id) as undoneCount from as_distribution_done where  distribution_done = ?`
		_, err := model.XormEngine.SQL(sql,0).Get(&undoneCount)
		if err!=nil{
			fmt.Println(err)
		}
		if undoneCount == 0{
			break
		}
		time.Sleep(time.Duration(1)*time.Second)
	}



	//portMap,inDegreeMap,outDegreeMap := GetPortAndDegree()
	AllPortMap ,AllIpMap,globalPeerInfo,globalSendPeerMap = GetPortAndIpMap()
	fmt.Println("ip和端口拉取完毕")

	//globalServer = make(chan int,1)
    //globalServer<- 0
    globalDeployServerNum = 0
    globalConnectServerNum = 0
    var connectServerNum = 0
    //globalDoneServer = make(chan int,1)
    //globalDoneServer<- len(freePort)

    globalDoneServerNum = int32(len(As_distribution_infos))

	var bgpList [] *model.BGP
    wg := &sync.WaitGroup{}
	for _,value := range As_distribution_infos{
		wg.Add(1)
		//sendNum := make(chan int,1)
      //sendNum <-value.Out_degree
      //receiveNum := make(chan int,1)
      //receiveNum <- value.In_degree
		sendNum := int32(value.Out_degree)
		receiveNum := int32(value.In_degree)
		peer,_ := globalPeerInfo.Load(value.Name)
		sendConn ,_ := globalSendPeerMap.Load(value.Name)
		if len(sendConn.([]string)) >0{
			connectServerNum += 1
		}
		bgp := &model.BGP{
			Port: value.Tcp_port,
			Name: value.Name,
			ProcessDone: make(chan bool),
			ConnectionDone : make(chan bool),
			SendNum: sendNum,
			ReceiveNum: receiveNum,
			Peer: peer.(sync.Map),
			SendConn: sendConn.([]string),
			SendConnNum: int32(len(sendConn.([]string))),
			GlobalIpMap: AllIpMap,
			GlobalPortMap: AllPortMap,
			}
		bgpList = append(bgpList,bgp)
		go bgp.ServerStart(wg,&globalDeployServerNum ,&globalConnectServerNum,&globalDoneServerNum)
	}
	allServerNum := len(As_distribution_infos)
	for {
      //serverNum := <- globalServer
      //globalServer<-serverNum
      serverNum := int(atomic.LoadInt32(&globalDeployServerNum))
      if serverNum == allServerNum{
      	as_distribution_done.Deployment_done = 1
			model.XormEngine.Table("as_distribution_done").ID(as_distribution_done.Id).Update(&as_distribution_done)
      		fmt.Println("server部署完毕")
          break
		}
    }

for {
		var deploymentUndoneCount int
		sql := `select count(id) as deploymentUndoneCount from as_distribution_done where  deployment_done = ?`
		_, err := model.XormEngine.SQL(sql,0).Get(&deploymentUndoneCount)
		if err != nil{
			fmt.Println("get deploymentUndoneCount err:",err)
		}
		if deploymentUndoneCount == 0{
			break
		}
		time.Sleep(time.Duration(1)*time.Second)
	}
	fmt.Println("server分发部署完毕，开始连接")
    for _,bgp_value := range bgpList{
    	go bgp_value.EchoConncttion()
	}

	for {
      serverConnectionNum := int(atomic.LoadInt32(&globalConnectServerNum))
      if serverConnectionNum == connectServerNum{
      	as_distribution_done.Connection_done = 1
			model.XormEngine.Table("as_distribution_done").ID(as_distribution_done.Id).Update(&as_distribution_done)
      		fmt.Println("server连接建立完毕")
          break
		}
    }



	go func() {
		for{
		   //serverNum := <- globalDoneServer
		   //globalDoneServer <- serverNum

		   serverNum := atomic.LoadInt32(&globalDoneServerNum)
		   if serverNum > 0{
			fmt.Println("收敛余额：",serverNum)
			}else{
				break
			}
			time.Sleep(time.Duration(5)*time.Second)
		}
	}()

    wg.Wait()
	as_distribution_done.Convergence_done = 1
	model.XormEngine.Table("as_distribution_done").ID(as_distribution_done.Id).Update(&as_distribution_done)
	fmt.Println("收敛")

	for {
		var convergenceUndoneCount int
		sql := `select count(id) as convergenceUndoneCount from as_distribution_done where  convergence_done = ?`
		_, err := model.XormEngine.SQL(sql,0).Get(&convergenceUndoneCount)
		if err != nil{
			fmt.Println("get convergenceUndoneCount err:",err)
		}
		if convergenceUndoneCount == 0{
			break
		}
		time.Sleep(time.Duration(2)*time.Second)
		fmt.Println("等待全局收敛" )

	}
    fmt.Println("全局收敛")

}