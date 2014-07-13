#encoding:utf-8
import sys
from subprocess import Popen, PIPE
import os

# 1. zk，2. nimbus，4. supervisor，8. storm_ui 16. ruby_web 32. mq
# 64. HMaster 128. ThriftServer 256. HRegionServer, 512.nginx
# 1024. NameNode 2048. ResourceManager 4096. DFSZKFailoverController, 8192. JobHistoryServer
# 16384. NodeManager  32768.DataNode  65536. JournalNode

#turnOnList = ['zk', 'nimbus', 'supervisor', 'storm_ui', 'ruby_web', 'mq', 'HMaster', 'ThriftServer', 'HRegionServer', 'nginx', 'NameNode', 'ResourceManager', 'DFSZKFailoverController', 'DFSZKFailoverController', 'JobHistoryServer', 'NodeManager', 'DataNode', 'JournalNode']
turnOnList = ['zk', 'nimbus', 'supervisor', 'storm_ui']
#turnOnList = ['zk', 'nginx', 'HMaster', 'NameNode', 'ThriftServer', 'JobHistoryServer', 'ResourceManager', 'DFSZKFailoverController']
#turnOnList = ['zk', 'NodeManager', 'DataNode', 'JournalNode', 'HRegionServer']

#/home/secneo/storm-ref/zookeeper-3.4.5/bin/../conf/zoo.cfg
#backtype.storm.daemon.nimbus
#backtype.storm.daemon.supervisor
#backtype.storm.daemon.work
#backtype.storm.ui.core
#ruby neo.rb
#netstat -an|grep 5672|grep LIST|wc -l

#org.apache.hadoop.hdfs.tools.DFSZKFailoverController
#org.apache.hadoop.yarn.server.resourcemanager.ResourceManager
#org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer
#org.apache.hadoop.hbase.thrift.ThriftServer
#org.apache.hadoop.hbase.master.HMaster
#org.apache.hadoop.hdfs.server.namenode.NameNode
#nginx: master process /opt/nginx/sbin/nginx
#
# org.apache.hadoop.yarn.server.nodemanager.NodeManager
# org.apache.hadoop.hdfs.server.datanode.DataNode
# org.apache.hadoop.hdfs.qjournal.server.JournalNode
# org.apache.hadoop.hbase.regionserver.HRegionServer

is_dubug_mode = False

def ps_cmd(progess) :
  return "ps -ef|grep -v 'grep'|grep '%s'|wc -l" % progess

def listent_cmd(port) :
  return "netstat -an|grep %d|grep LISTEN|wc -l" % port

def error_msg(key_name) :
  return "%s SHUT DOWN!" % key_name

#Mydict item format : [ERROR_CODE, SHELL_CMD, SHELL_RETURN_VALUE_ASSERT_FUNCTION, ERROR_MSG]
Mydict = {
  'zk' :         (1, listent_cmd(2181), lambda x : int(x) > 0),
  'nimbus' :     (2, ps_cmd('backtype.storm.daemon.nimbus'), lambda x : int(x) > 0),
  'supervisor' : (4, ps_cmd('backtype.storm.daemon.supervisor'), lambda x : int(x) > 0),
  'storm_ui' :   (8, ps_cmd('backtype.storm.ui.core'), lambda x : int(x) > 0),
  'ruby_web' :   (16, ps_cmd("ruby neo.rb"), lambda x : int(x) > 0),
  'mq' :         (32, listent_cmd(5672), lambda x : int(x) > 0),
  'HMaster' :           (64, ps_cmd('org.apache.hadoop.hbase.master.HMaster'), lambda x : int(x) > 0),
  'ThriftServer' :      (128, ps_cmd('org.apache.hadoop.hbase.thrift.ThriftServer'), lambda x : int(x) > 0),
  'HRegionServer' :     (256, ps_cmd('org.apache.hadoop.hbase.regionserver.HRegionServer'), lambda x : int(x) > 0),
  'nginx' :             (512, ps_cmd('nginx: master process'), lambda x : int(x) > 0),
  'NameNode' :          (1024, ps_cmd('org.apache.hadoop.hdfs.server.namenode.NameNode'), lambda x : int(x) > 0),
  'ResourceManager' :   (2048, ps_cmd('org.apache.hadoop.yarn.server.resourcemanager.ResourceManager'), lambda x : int(x) > 0),
  'DFSZKFailoverController' :   (4096, ps_cmd('org.apache.hadoop.hdfs.tools.DFSZKFailoverController'), lambda x : int(x) > 0),
  'JobHistoryServer' :  (8192, ps_cmd('org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer'), lambda x : int(x) > 0),
  'NodeManager' :       (16384, ps_cmd('org.apache.hadoop.yarn.server.nodemanager.NodeManager'), lambda x : int(x) > 0),
  'DataNode' :          (32768, ps_cmd('org.apache.hadoop.hdfs.server.datanode.DataNode'), lambda x : int(x) > 0),
  'JournalNode' :       (65536, ps_cmd('org.apache.hadoop.hdfs.qjournal.server.JournalNode'), lambda x : int(x) > 0)
}


def mycall(key) :
  entry = Mydict[key]
  cmd = entry[1]
  check_fn = entry[2]
  error_txt = error_msg(key)
  proc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
  out, err = proc.communicate()
  exitcode = proc.returncode
  if (is_dubug_mode) :
    print(cmd[0:-6])
    os.system(cmd[0:-6])
    print "    [int(out), out, err, exitcode, check_fn(out)] = [%d,%s,%d,%s]" % (int(out), err, exitcode, check_fn(out))
  return {True:entry[0], False:0}[exitcode != 0 or check_fn(out) == False]

  # if (exitcode != 0 or check_fn(out) == False) :
  #   return "%s, %s" % (error_txt, err)

out_list = map(mycall, turnOnList)

# out_string = '\n'.join(filter(lambda x : x != None, out_list)).strip()
# if (len(out_string) > 0) :
#   print out_string
#sys.exit({True:0, False:1}[len(out_string) == 0])

out_value = reduce(lambda x, y : x + y, out_list)
print out_value
sys.exit({True:0, False:1}[out_value == 0])
