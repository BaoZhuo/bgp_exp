package main

import (

	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"github.com/go-xorm/core"
	"os"
	"strings"
	"sync"

	"math"
	"net"

	"time"

	//"os"

)
var XormEngine = &xorm.Engine{}


type As_distribution_done struct {
	Id           int `json:"id" xorm:"id int pk autoincr"`
	Container_num int `json:"Container_num" xorm:"container_num int"`
	Container_name string `json:"Container_name" xorm:"container_name varchar"`
	Ip string `json:"ip" xorm:"ip varchar"`
	Distribution_done int `json:"distribution_done" xorm:"distribution_done tinyint"`
	Deployment_done int `json:"deployment_done" xorm:"deployment_done tinyint"`
	Convergence_done int `json:"convergence_done" xorm:"convergence_done tinyint"`

}
type As_distribution_info struct {
	Id           int `json:"id" xorm:"id int pk autoincr"`
	Name string `json:"name" xorm:"name varchar"`
	Tcp_port int `json:"tcp_port" xorm:"tcp_port int"`
	Ip string `json:"ip" xorm:"ip varchar"`
	In_degree int `json:"in_degree" xorm:"in_degree int"`
	Out_degree int `json:"out_degree" xorm:"out_degree int"`
	Container_name string `json:"Container_name" xorm:"container_name varchar"`
}

var AllPortMap map[string]int
var AllIpMap map[string]string
var globalServer chan int



type BGP struct{
    port int
    name string
    processDone chan bool
    sendNum chan int
    receiveNum chan int
}


func (bgp *BGP) Start(wg *sync.WaitGroup)  {
    wg.Add(1)
    go bgp.ServerStart(wg)
}

func (bgp *BGP) DoServerStuff(conn net.Conn) {

    buf := make([]byte, 512)
    buf_len, err := conn.Read(buf)
    if err != nil {
        fmt.Println("Error reading", err.Error())
        return
    }
    asPathContent := strings.Split(string(buf[:buf_len]),"|")

    for index,value := range asPathContent{
        if value == bgp.name {
            //处理验证
            receiveNum := <- bgp.receiveNum -1
            bgp.receiveNum <- receiveNum
            sendNum := <-bgp.sendNum

                 //bgpsec
             times := index + 2
             //ASPA
             //times := 1
             //if index % 2 == 0{
             //  times = 2
             //}
             time.Sleep(time.Duration(times*10)*time.Millisecond)


            if index < len(asPathContent) - 1{
                //处理发送-签名
                //fmt.Println("------------------我是",bgp.name,"收到内容", string(buf[:buf_len]),"要发送",asPathContent[index+1],"端口号",AllPortMap[asPathContent[index+1]],
                //	"ip:",AllIpMap[asPathContent[index+1]])
                bgp.SendToPeer(AllIpMap[asPathContent[index+1]],AllPortMap[asPathContent[index+1]],string(buf[:buf_len]))
                sendNum = sendNum -1
            }else{
                //fmt.Println("~~~~~~~~~~~~~~~~~~我是终点",bgp.name,"收到内容", string(buf[:buf_len]))
            }
            bgp.sendNum <- sendNum
            if sendNum < 1 && receiveNum < 1{
                bgp.processDone <- true
             }

            break
        }
    }
    conn.Close()
}
func (bgp *BGP) ServerStart(wg *sync.WaitGroup)    {
    defer wg.Done()
    go bgp.ServerListen()
    wg1 := &sync.WaitGroup{}
    wg1.Add(1)
    go bgp.CheckDone(wg1)
    wg1.Wait()
}

func (bgp *BGP) CheckDone(wg *sync.WaitGroup)    {
    defer wg.Done()
    for{
        value:= <-bgp.processDone
        if value == true{
            return
        }
    }
}

func (bgp *BGP) ServerListen()  {
   address := fmt.Sprintf("0.0.0.0:%d", bgp.port)
   listener, _ := net.Listen("tcp", address)
   defer listener.Close()
   //if err != nil {
   //  fmt.Println("Error listening", err.Error())
   //  return
   //}
   serverNum := <- globalServer + 1
   globalServer <- serverNum
   //fmt.Println("启动服务:",serverNum)
   for {
       //fmt.Printf("Pointer: %p\n", bgp)
       conn, err := listener.Accept()
       //defer conn.Close()
       if err != nil {
           fmt.Println("Error accepting", err.Error())
           return
       }
       go bgp.DoServerStuff(conn)
    }
   }


func (bgp *BGP) SendToPeer(peerIp string,peerPort int,asPathContent string)  {
    peerAdress := fmt.Sprintf("%s:%d", peerIp, peerPort)
    conn, err := net.Dial("tcp", peerAdress)
    //defer conn.Close()
    if err != nil {
        fmt.Println("发送失败", err.Error())
        return
    }

    _, err = conn.Write([]byte(asPathContent))
    conn.Close()
    return

}




func externalIP() (net.IP, error) {
    ifaces, err := net.Interfaces()
    if err != nil {
        return nil, err
    }
    for _, iface := range ifaces {
        if iface.Flags&net.FlagUp == 0 {
            continue // interface down
        }
        if iface.Flags&net.FlagLoopback != 0 {
            continue // loopback interface
        }
        addrs, err := iface.Addrs()
        if err != nil {
            return nil, err
        }
        for _, addr := range addrs {
            ip := getIpFromAddr(addr)
            if ip == nil {
                continue
            }
            return ip, nil
        }
    }
    return nil, errors.New("connected to the network?")
}

func InitMySqlParameter(user, password, server, database string) (engine *xorm.Engine, err error) {
	openSql := user + ":" + password + "@tcp(" + server + ")/" + database + "?charset=utf8&parseTime=True&loc=Local"
	engine, err = xorm.NewEngine("mysql", openSql)
	if err != nil {
		fmt.Println("NewEngine err",err)
		return engine, err
	}
	engine.SetTableMapper(core.SnakeMapper{})
	return engine, nil
}


func getIpFromAddr(addr net.Addr) net.IP {
    var ip net.IP
    switch v := addr.(type) {
    case *net.IPNet:
        ip = v.IP
    case *net.IPAddr:
        ip = v.IP
    }
    if ip == nil || ip.IsLoopback() {
        return nil
    }
    ip = ip.To4()
    if ip == nil {
        return nil // not an ipv4 address
    }

    return ip
}

func InitMysql()  {
	user := "root"
	password := "Rpstir-123"
	server := "202.173.14.103:23306"
	database := "bgp_exp"
	XormEngine, _ = InitMySqlParameter(user, password, server, database)
}
func GetFreePorts(count int) ([]int, error) {
   var ports []int

   m := make(map[int]bool) //map的值不重要

   for{
   	   	l, _ := net.Listen("tcp", ":0")
   	   	defer l.Close()
   	   	port := l.Addr().(*net.TCPAddr).Port
		if _, ok := m[port]; !ok{
			   ports = append(ports, port)
			   m[port] = true
		}
		if len(ports) >=count{
			break
		}
   }

   //
   //
   //for i := 0; i < count; i++ {
   //    //addr, _ := net.ResolveTCPAddr("tcp", "localhost:0")
   //    //l, _ := net.ListenTCP("tcp", addr)
   //    //fmt.Printn("端口空闲",i,l.Addr().(*net.TCPAddr).Port)
   //    //defer l.Close()
   //	   	l, _ := net.Listen("tcp", ":0")
   //	   	defer l.Close()
   //	   	port := l.Addr().(*net.TCPAddr).Port
   //		fmt.Println(port,i)
   //    ports = append(ports, port)
   //}
   return ports, nil
}

func GetPortAndIpMap()(ipPost map[string] string,portMap map[string] int){

	portMap = make(map[string] int)
	ipPost = make(map[string] string)
	sql := `SELECT * FROM as_distribution_info`
	var As_distribution_infos []As_distribution_info
	err := XormEngine.SQL(sql).Find(&As_distribution_infos)
	if err != nil{
		fmt.Println("get As_distribution_infos err:",err)
	}
	for _,value := range As_distribution_infos{
		portMap[value.Name] = value.Tcp_port
		ipPost[value.Name] = value.Ip
	}
	return
}




func main() {
	time.Sleep(time.Duration(10)*time.Second)
	InitMysql()
	defer XormEngine.Close()
	ip, err := externalIP()
	if err != nil{
		fmt.Println("getIP err:",err)
	}
	name, err := os.Hostname()
	if err != nil{
		fmt.Println("getIP err:",err)
	}
	//name := "c1ad44d8c58a"
	fmt.Println("ip：",ip.String(),"name,",name)


	session := XormEngine.NewSession()
	if err := session.Begin(); err != nil {
		session.Rollback()
		fmt.Println(session, "session.Begin() fail", err)
	}
	//sqlStr := `Update as_distribution_done (container_name,ip) VALUES  (?)`
	//res, err := session.Exec(sqlStr, "zhang")

	//得到所有记录的长度
	var asLength int
	sql := `select count(id) as asLength from as_distribution_info `
	_, err = XormEngine.SQL(sql).Get(&asLength)
	if err != nil{
		fmt.Println("getAllLength err:",err)
	}
	fmt.Println("asLength,",asLength)
	//得到容器个数
	var containerNum int
	sql = `select count(id) as containerNum from as_distribution_done `
	_, err = XormEngine.SQL(sql).Get(&containerNum)
	if err != nil{
		fmt.Println("get container Length err:",err)
	}
	fmt.Println("containerNum:,",containerNum)
	//得到自己的序号
	as_distribution_done := As_distribution_done{}
	sql = `select * from as_distribution_done `
	_, err = XormEngine.Table("as_distribution_done").Where(" container_name = ?",name).Get(&as_distribution_done)
	if err != nil{
		fmt.Println("get As_distribution_done err:",err)
	}
	fmt.Println("as_distribution_done,",as_distribution_done)
	//计算分区
	perCount := int(math.Ceil(float64(asLength)/float64(containerNum)))
	startIndex := perCount*(as_distribution_done.Container_num)
	sql = `SELECT * FROM as_distribution_info order by id asc LIMIT ?,?`
	fmt.Println("sql:",sql,"startindex",startIndex,"perCount",perCount)
	var As_distribution_infos []As_distribution_info
	err = XormEngine.SQL(sql,startIndex,perCount).Find(&As_distribution_infos)
	if err != nil{
		fmt.Println("get As_distribution_infos err:",err)
	}
	fmt.Println("计算得到分区，分区数量",len(As_distribution_infos))
	freePort,err := GetFreePorts(len(As_distribution_infos))
	fmt.Println("计算得到分区，端口数量",len(freePort))
	for index,_ := range As_distribution_infos{
		As_distribution_infos[index].Tcp_port = freePort[index]
		As_distribution_infos[index].Ip = ip.String()
		As_distribution_infos[index].Container_name = name
		//fmt.Println("更新,",As_distribution_infos[index])
		XormEngine.Table("as_distribution_info").ID(As_distribution_infos[index].Id).Update(&As_distribution_infos[index])
	}
	fmt.Println("分区ip端口更新完毕")
	as_distribution_done.Distribution_done = 1
	as_distribution_done.Ip = ip.String()

	XormEngine.Table("as_distribution_done").ID(as_distribution_done.Id).Update(&as_distribution_done)
	fmt.Println(as_distribution_done)

	err = session.Commit()
	if err != nil{
		fmt.Println("commit err:",err)
	}

	for {
		var undoneCount int
		sql = `select count(id) as undoneCount from as_distribution_done where  distribution_done = ?`
		_, err = XormEngine.SQL(sql,0).Get(&undoneCount)
		if undoneCount == 0{
			break
		}
		time.Sleep(time.Duration(1)*time.Second)
	}
	//portMap,inDegreeMap,outDegreeMap := GetPortAndDegree()
	AllIpMap,AllPortMap = GetPortAndIpMap()
	fmt.Println("ip和端口拉取完毕")

	globalServer = make(chan int,1)
    globalServer<- 0

    wg := &sync.WaitGroup{}
	for _,value := range As_distribution_infos{
		wg.Add(1)
		sendNum := make(chan int,1)
        sendNum <-value.Out_degree
        receiveNum := make(chan int,1)
        receiveNum <- value.In_degree
		go (&BGP{port: value.Tcp_port,name: value.Name,processDone: make(chan bool),sendNum: sendNum,receiveNum: receiveNum}).ServerStart(wg)
	}
	allServerNum := len(As_distribution_infos)
	for {
        serverNum := <- globalServer
        globalServer<-serverNum
        if serverNum == allServerNum{
        	as_distribution_done.Deployment_done = 1
			XormEngine.Table("as_distribution_done").ID(as_distribution_done.Id).Update(&as_distribution_done)
        	fmt.Println("server部署完毕")
            break
		}
    }

    wg.Wait()
	as_distribution_done.Convergence_done = 1
	XormEngine.Table("as_distribution_done").ID(as_distribution_done.Id).Update(&as_distribution_done)




}