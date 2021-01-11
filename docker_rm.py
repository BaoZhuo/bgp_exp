import subprocess

num = 50



slave = ""
for index in range(num):
    slave += "alpine_%d " % (index)
stop_slave_cmd = 'docker stop ' + slave
stop_send_cmd = 'docker stop handle'



rm_slave_cmd = 'docker rm ' + slave
rm_send_cmd = 'docker rm handle'

status, output = subprocess.getstatusoutput(stop_slave_cmd)
if status != 0:
    print("cmd", "reason:", output)
status, output = subprocess.getstatusoutput(stop_send_cmd)
if status != 0:
    print("cmd",  "reason:", output)
status, output = subprocess.getstatusoutput(rm_slave_cmd)
if status != 0:
    print("cmd",  "reason:", output)
status, output = subprocess.getstatusoutput(rm_send_cmd)
if status != 0:
    print("cmd",  "reason:", output)