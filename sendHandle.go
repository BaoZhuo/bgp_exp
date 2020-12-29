package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"github.com/go-xorm/core"
	"time"
)
var fileName = flag.String("fileName", "", "文件路径")
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

type Handle struct {
    asPathContents []string
    portMap map[string] int
    ipMap map[string] string
    //sendNumMap map[string] int
    //receiveNumMap map[string] int
}
func (handle *Handle) NewClient(wg *sync.WaitGroup,asPathContent string,port int,ip string)  {
    defer wg.Done()
    address := fmt.Sprintf("%s:%d",ip,port)
    conn, err := net.Dial("tcp", address)
    if err != nil {
        fmt.Println("--------------------Error dialing", err.Error(),address)
        return // 终止程序
    }
    _, err = conn.Write([]byte(asPathContent))
    return
}

func (handle *Handle) GetStartAs(asPathContent string) (startAs string){
    asPathContents := strings.Split(asPathContent,"|")
    if len(asPathContents) > 0{
        return asPathContents[0]
    }
    return
}
func (handle *Handle) Start(Wg *sync.WaitGroup)  {
	defer Wg.Done()
    wg := &sync.WaitGroup{}
    for _,value := range handle.asPathContents{
        startAs := handle.GetStartAs(value)
        port := handle.portMap[startAs]
        ip := handle.ipMap[startAs]
        wg.Add(1)
        go handle.NewClient(wg,value,port,ip)
    }
    wg.Wait()
}
func InitMysql()  {
	user := "root"
	password := "Rpstir-123"
	server := "202.173.14.103:23306"
	database := "bgp_exp"
	XormEngine, _ = InitMySqlParameter(user, password, server, database)
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
func GetPortMap()(portMap map[string] int,ipMap map[string] string){
	portMap = make(map[string] int)
	ipMap = make(map[string] string)
	sql := `SELECT * FROM as_distribution_info`
	var As_distribution_infos []As_distribution_info
	err := XormEngine.SQL(sql).Find(&As_distribution_infos)
	if err != nil{
		fmt.Println("get As_distribution_infos err:",err)
	}
	for _,value := range As_distribution_infos{
		portMap[value.Name] = value.Tcp_port
		ipMap[value.Name] = value.Ip
	}
	return
}

func GetAsPathContens()(asPathcontent []string){
	flag.Parse()
	filename := *fileName
	csvFile, _ := os.Open(filename)
    //csvFile, _ := os.Open("./data/test.csv")
    reader := csv.NewReader(bufio.NewReader(csvFile))
    m := make(map[string]bool)
    for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}

		//csvLine,csvLineArray := ProcessCsvLine(line[0])
		csvLine := line[3]
		if len(csvLine) == 0{
		    continue
        }
		if _, ok := m[csvLine]; !ok {
		    asPathcontent = append(asPathcontent,csvLine)
		    m[csvLine] = true
        }
	}
	asPathcontent = asPathcontent[1:]
	return

}

func main(){
	time.Sleep(time.Duration(10)*time.Second)
	InitMysql()
	defer XormEngine.Close()
	for {
		var distributionUndoneCount int
		sql := `select count(id) as distributionUndoneCount from as_distribution_done where  distribution_done = ?`
		_, err := XormEngine.SQL(sql,0).Get(&distributionUndoneCount)
		if err != nil{
			fmt.Println("get distributionUndoneCount err:",err)
		}
		if distributionUndoneCount == 0{
			break
		}
		time.Sleep(time.Duration(1)*time.Second)
	}
    portMap,ipMap := GetPortMap()
    asPathcontents := GetAsPathContens()
    fmt.Println("AS个数，",len(portMap),"发包长度，",len(asPathcontents))
	wg := &sync.WaitGroup{}
	handle := &Handle{asPathContents:asPathcontents,portMap : portMap,ipMap: ipMap}
	wg.Add(1)

	for {
		var deploymentUndoneCount int
		sql := `select count(id) as deploymentUndoneCount from as_distribution_done where  deployment_done = ?`
		_, err := XormEngine.SQL(sql,0).Get(&deploymentUndoneCount)
		if err != nil{
			fmt.Println("get deploymentUndoneCount err:",err)
		}
		if deploymentUndoneCount == 0{
			break
		}
		time.Sleep(time.Duration(1)*time.Second)
	}
	fmt.Println("server分发并部署完毕，开始")
	start := time.Now()
    handle.Start(wg)
    wg.Wait()
    fmt.Println("全局收敛")
    elapsed := time.Since(start)
    fmt.Println("收敛完成耗时：", elapsed)

}