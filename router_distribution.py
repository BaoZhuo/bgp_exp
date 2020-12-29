import os
import subprocess
import time

import pandas as pd
import numpy as np
import pymysql

db = pymysql.Connect(
    host='202.173.14.103',
    port=23306,
    user='root',
    passwd='Rpstir-123',
    db='bgp_exp',
    charset='utf8'
)
cursor = db.cursor()
cursor.execute("truncate table as_distribution_info ")
cursor.execute("truncate table as_distribution_done ")

file_name = "./data/bgpstream/route-views.isc_2020-12-01_00:00:00_2020-12-02_00:00:00.csv"
new_file_name = "./data/bgpstream/route-views.isc_2020-12-01_00:00:00_2020-12-02_00:00:00_cleaned.csv"

def data_clean():
    data = pd.read_csv(file_name)
    data.dropna(axis=0, how='any', inplace=True)
    data['path'] = data['path'].apply(lambda i: i.replace(",", " ").split(' ')[::-1])
    data['path'] = data['path'].apply(lambda i: list(filter(lambda x: i.count(x) == 1, i)))
    data['path'] = data['path'].apply(lambda i: ['AS' + i[index] for index in range(len(i))])
    data['as_path'] = data['path'].apply(lambda i: "|".join(i))
    data.drop_duplicates(subset='as_path', keep='first', inplace=True)
    data.to_csv(new_file_name,index=False)
    return data
def get_degree_map(data):
    as_path = list(data['path'])
    in_degree_map = {}
    out_degree_map = {}
    for value in as_path:
        for index in range(len(value)):
            #in_degree
            try:
                in_degree_map[value[index]] += 1
            except:
                in_degree_map[value[index]] = 1

            if (index + 1 < len(value)):
                try:
                    out_degree_map[value[index]] += 1
                except:
                    out_degree_map[value[index]] = 1
            else:
                out_degree_map[value[index]] = 0
    return in_degree_map,out_degree_map

def router_distribution():
    data_pd = data_clean()
    in_degree_map,out_degree_map = get_degree_map(data_pd)
    as_set = list(set([i for item in list(data_pd['path']) for i in item]))
    for index in range(len(as_set)):
        #print(index/len(as_set)*100)
        sql = "INSERT INTO as_distribution_info (name,in_degree,out_degree) VALUES  ('%s','%d','%d')"
        data = (as_set[index])
        re = cursor.execute(sql % (data,in_degree_map[data],out_degree_map[data]))
    db.commit()
    print("as_distribution_info插入完毕，事务已经提交")
    return in_degree_map, out_degree_map
    #data = cursor.fetchone()






def CMD(cmd):
    status, output = subprocess.getstatusoutput(cmd)
    if status != 0:
        print("rm crt faild, cmd", cmd, "reason:", output)
        return
    print(output)
    return




def main():
    in_degree_map, out_degree_map = router_distribution()

    cur_path =  os.path.abspath(os.path.dirname(__file__))





    #启动server，进行server资源分发
    docker_num = 2
    for index in range(docker_num):
        cmd = "touch %s/alpine_%d.log && \
        docker run -v %s/slaver:/slaver  \
        --expose=1020-60000\
        -v %s/alpine_%d.log:/alpine_%d.log  \
        --name alpine_%d -itd alpine \
        /bin/sh  -c '/slaver>alpine_%d.log'" % (cur_path ,index,cur_path ,cur_path ,index ,index ,index ,index)
        print(cmd)
        status, output = subprocess.getstatusoutput(cmd)
        if status != 0:
            print("rm crt faild, cmd", cmd, "reason:", output)
        docker_id = output.strip()[:12]
        sql = "INSERT INTO as_distribution_done (container_name,container_num) VALUES  ('%s','%d')" % (docker_id,index)

        cursor.execute(sql)





    db.commit()
    print("提交结束")

    #执行handle
    cmd = "touch %s/handle.log &&\
    docker run -itd -v %s/sendHandle:/sendHandle  \
    --expose=1020-60000\
    -v %s/data:/data  \
    -v %s/handle.log:/handle.log \
    --name handle  alpine \
    /bin/sh  -c '/sendHandle -fileName='%s' >handle.log' " % (cur_path,cur_path,cur_path,cur_path,new_file_name)
    print("handle执行完毕")
    print(cmd)
    status, output = subprocess.getstatusoutput(cmd)
    if status != 0:
        print("rm crt faild, cmd", cmd, "reason:", output)


    while True:
        sql = "select count(id) as un_convergenced_count from as_distribution_done where convergence_done = ''"
        un_convergenced_count = cursor.fetchone()
        if un_convergenced_count == 0:
            print("全局收敛")
            break
        time.sleep(5)
    cursor.close()
    db.close()

    return




if __name__ == '__main__':
    main()