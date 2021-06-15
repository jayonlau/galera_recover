#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
本脚本适用于X86/FT/SW等平台
一、使用方法
    拷贝本脚本到任意mariadb集群节点，执行 python galera_recover_XXX.py
二、适用情况
    1、三节点都无法启动，且均报错failed to reach primary view: 110 (Connection timed out)
二、特别说明
    1、为保障数据安全，本脚本执行过程中，如果要重新指定primary节点启动，会对mariadb整个数据文件夹备份，具体为/etc/kolla/日期，后期可以删除。
"""
import time
import os
import sys


#grastate.dat、gvwstate.dat文件路径
grastate_file = "/var/lib/docker/volumes/mariadb/_data/grastate.dat"
gvwstate_file = "/var/lib/docker/volumes/mariadb/_data/gvwstate.dat"


# 获取当前galera的集群的各节点的ip
node_ips_info = os.popen("cat /etc/kolla/mariadb/galera.cnf |grep '^wsrep_cluster_address'").read()
node_ips_str = node_ips_info.split('gcomm://')[1]
node_ips_str = node_ips_str.strip()
node_ips_str = node_ips_str.replace(':4567','')
node_ips_arr = node_ips_str.split(',')



def backup_dir():
    """备份_data文件夹"""
    date = time.strftime("%Y-%m-%d")
    Mariadb_dir = '/etc/kolla/' + date
    for node_ip in node_ips_arr:
        free_disk_var = os.popen('ssh ' + node_ip + " df -m / | awk '{print $4}' | grep '^[0-9]'").read()
        free_disk_var = int(free_disk_var) - 8192
        data_disk = os.popen('ssh ' + node_ip + " du -sm /var/lib/docker/volumes/mariadb/_data  | awk '{print $1}'").read()
        if not os.path.exists(Mariadb_dir):
           if int(data_disk) < int(free_disk_var):
              os.popen('ssh ' + node_ip + ' mkdir ' + Mariadb_dir).read()
              os.system('ssh ' + node_ip + ' " tar -zcvf  ' + Mariadb_dir + "/mariadb.tar.gz /var/lib/docker/volumes/mariadb/_data"'" >>/dev/null 2>&1')
              time.sleep(2)
              print("%s_data备份成功。"%node_ip)
           else:
              print("备份空间不足，建议清理部分空间用以备份_data文件夹···")
        else:
           print("_data备份文件已经存在于/etc/kolla/日期目录下")


def start_mariadb_with_wsrep(ip):
    """设定集群发起人角色"""
    bootstrap = os.popen('ssh ' + ip + ' cat /var/lib/docker/volumes/mariadb/_data/grastate.dat').read()
    if "safe_to_bootstrap" in bootstrap:
       modify_boot = " sed -i 's/safe_to_bootstrap: 0/safe_to_bootstrap: 1/' /var/lib/docker/volumes/mariadb/_data/grastate.dat"
       os.popen("ssh " + ip + ' "' + modify_boot + '"')

    modify_conf = " sed -i 's/mysqld_safe/mysqld_safe --wsrep-new-cluster/' /etc/kolla/mariadb/config.json"
    restore_conf = " sed -i 's/mysqld_safe --wsrep-new-cluster/mysqld_safe/' /etc/kolla/mariadb/config.json"
    print("修改启动参数中···")
    os.popen("ssh " + ip + ' "' + modify_conf + '"')
    time.sleep(1)
    print("primary节点启动中···")
    os.popen('ssh ' + ip + ' docker restart mariadb').read()
    # 将配置文件恢复回去
    time.sleep(10)
    print("恢复启动参数中····")
    os.popen("ssh " + ip + ' "' + restore_conf + '"' )
    if check_mariadb_active_now(ip) is True:
        print("primary节点启动成功。")
    else:
        print('请人工处理，脚本无法完成修复。')

def check_mariadb_active_now(galera_node_ip):
    """检测mariadb服务是否启动"""
    """haproxy的3306端口排除，若还存在mariadb的3306表明mariadb启动了"""
    """检测两次"""
    is_active1 = os.popen('ssh ' + galera_node_ip + ' netstat -tnlp | grep :3306 | grep -v haproxy').read()
    is_active1 = "3306" in is_active1
    time.sleep(3)
    is_active2 = os.popen('ssh ' + galera_node_ip + ' netstat -tnlp | grep :3306 | grep -v haproxy').read()
    is_active2 = "3306" in is_active2
    if is_active1 and is_active2:
       return True
    return False

def get_all_nodes_seqno():
    """获取所有节点的seqno值"""
    seqno_dict = {}
    for node_ip in node_ips_arr:
        ls_grastate = os.system('ssh ' + node_ip + ' ls ' + grastate_file + ' > /dev/null')
        if ls_grastate != 512:
           node_seqno = os.popen('ssh ' + node_ip + ' cat ' + grastate_file + ' | grep seqno').read()
           node_seqno = node_seqno.replace('seqno:','')
           seqno_dict[node_ip] = node_seqno
        else:
           seqno_dict[node_ip] = int(-1)
    return(seqno_dict)

def get_safe_to_bootstrap():
    """获取safe_to_bootstrap值为1的节点"""
    bootstrap_dict = {}
    for node_ip in node_ips_arr:
        ls_grastate = os.system('ssh ' + node_ip + ' ls ' + grastate_file + ' > /dev/null')
        if ls_grastate != 512:
           safe_to_bootstrap = os.popen('ssh ' + node_ip + ' cat ' + grastate_file + ' | grep safe_to_bootstrap').read()
           if safe_to_bootstrap == '':
              safe_to_bootstrap = int(0)
              bootstrap_dict[node_ip] = safe_to_bootstrap 
           else:
              safe_to_bootstrap = safe_to_bootstrap.replace('safe_to_bootstrap:','')
              bootstrap_dict[node_ip] = safe_to_bootstrap
        else:
           bootstrap_dict[node_ip] = int(0)
    return(bootstrap_dict)

def get_node_uv_is_equal():
    """获取相同节点的uuid值"""
    for node_ip in node_ips_arr:
        if not os.path.exists(gvwstate_file):
           print("%s节点uuid信息不存在"%node_ip)
        else:
           view_id = os.popen('ssh ' + node_ip + ' cat ' + gvwstate_file + ' | grep view').read()
           my_uuid = os.popen('ssh ' + node_ip + ' cat ' + gvwstate_file + ' | grep my_uuid').read()
           my_uuid = my_uuid.replace('my_uuid: ','')
           if my_uuid in view_id:
              uv_equal_ip = node_ip
              return(uv_equal_ip)

def start_slave_mariadb(primary_ip):
    """启动非primary的其他节点"""
    node_ips_arr2 = node_ips_arr[:]
    node_ips_arr2.remove(primary_ip)
    for slave_node in node_ips_arr2:
        os.system('ssh ' + slave_node + ' docker restart mariadb')
        time.sleep(50)
        if check_mariadb_active_now(slave_node):
           print("slave节点 %s 数据库服务已经启动成功！"%slave_node)
        else:
           print("slave节点 %s 数据库服务没有启动成功，请检查！"%slave_node)


def stop_all_mariadb():
    """关闭所有节点的mariadb服务"""
    os.system('ssh ' + node_ips_arr[2] + ' docker stop mariadb')
    time.sleep(2)
    os.system('ssh ' + node_ips_arr[0] + ' docker stop mariadb')
    time.sleep(2)
    os.system('ssh ' + node_ips_arr[1] + ' docker stop mariadb')
    time.sleep(2)

def galera_recover():

    """判断集群内所有地址是否都在线"""
    if node_ips_arr == [""]:
       print("单节点环境不适用脚本")
       sys.exit(0)
    for node_ip in node_ips_arr:
        rep = os.popen('ping -c 3 -w 10 ' + node_ip + ' | grep time= | wc -l' ).read()
        print("测试数据库主机%s是否在线······"%node_ip)
        if rep == '0':
            print('数据库主机 %s 网络不可达，请检查!'%node_ip)
            sys.exit(0)
    print('所有数据库主机都在线！')

    """判断集群内是否有mariadb主机在线"""
    down_node_ip = []
    up_node_ip = []
    for node_ip in node_ips_arr:
        if check_mariadb_active_now(node_ip):
           print("数据库 %s 服务已经是启动状态！"%node_ip)
           up_node_ip.append(node_ip)
           if len(up_node_ip) == 3:
              print("所有数据库节点都启动了，不需要恢复！")
              break
        else:
           down_node_ip.append(node_ip)
     #如果集群内存在启动着的mariadb
    if up_node_ip and down_node_ip:
       print("%s中的数据库服务没有启动"%down_node_ip)
       for down_ip in down_node_ip:
           print("重启%s数据库中"%down_ip)
           os.system('ssh ' + down_ip + ' docker restart mariadb')
           time.sleep(10)
           if check_mariadb_active_now(down_ip):
              print("mariadb %s 启动成功！"%down_ip)
              if check_mariadb_active_now(node_ips_arr[0]) and check_mariadb_active_now(node_ips_arr[1]) and check_mariadb_active_now(node_ips_arr[2]):
                 print("数据库全部启动成功。")
                 sys.exit(0)
           else:
              print("mariadb %s 没有启动成功！"%down_ip)


    """这里针对的是存在一个初始化状态节点的处理，先关闭这个节点，grastate中qeqno值会变为-1"""
    if len(up_node_ip) == 1 or len(down_node_ip) == 3:
       for node_ip in node_ips_arr:
           if check_mariadb_active_now(node_ip):
              print("关闭mariadb节点%s服务"%node_ip)
              os.system('ssh ' + node_ip + ' docker stop mariadb')
              os.system('ssh ' + node_ip + ' rm -rf ' + grastate_file + ' > /dev/null')
              time.sleep(10)

     #如果集群内不存在启动着的mariadb
    if len(down_node_ip) == 3 or len(up_node_ip) == 1:
       print("三个mariadb节点都挂了，开启修复流程！")
       backup_dir()
       """根据bootstrap"""
       bootstrap_dict = get_safe_to_bootstrap()
       print("bootstrap信息情况为%s"%bootstrap_dict)
       for key in bootstrap_dict:
           if int(bootstrap_dict[key]) == int(1):
               first_boot_node = key
               print("通过safe_bootstrap值%s节点将为primary节点启动集群！！!"%first_boot_node)
               start_mariadb_with_wsrep(first_boot_node)
               start_slave_mariadb(first_boot_node)
               sys.exit(0)
       """根据seqno值选举集群发起人"""
       max_seqno = int(-1)
       seqno_dict = get_all_nodes_seqno()
       print("seqno信息情况为%s"%seqno_dict)
       for key in seqno_dict:
           if int(seqno_dict[key]) > int(max_seqno):
              max_seqno = int(seqno_dict[key])
              first_boot_node = key
       if max_seqno != -1:
          print("通过seqno值%s节点将为primary节点启动集群！！!"%first_boot_node)
          start_mariadb_with_wsrep(first_boot_node)
          start_slave_mariadb(first_boot_node)
       """如果所有的节点seqno值都为-1"""
       if max_seqno == -1:
          print("无法通过seqno值判断出primary节点")
          uv_equal_ip = get_node_uv_is_equal()
          if uv_equal_ip != None:
             print("通过UUId选择%s为primay节点启动中···"%uv_equal_ip)
             start_mariadb_with_wsrep(uv_equal_ip)
             start_slave_mariadb(uv_equal_ip)
          else:
             print("无法通过UUID判断parimary节点，自选启动中···")
             print("%s,作为集群发起人启动中"%node_ips_arr[2])
             chose_pri_ip = node_ips_arr[2]
             start_mariadb_with_wsrep(chose_pri_ip)
             start_slave_mariadb(chose_pri_ip)
             if check_mariadb_active_now(node_ips_arr[0]) and check_mariadb_active_now(node_ips_arr[1]) and check_mariadb_active_now(node_ips_arr[2]):
                print("数据库全部启动成功。")
             else:
                print("%s,作为集群发起人启动中"%node_ips_arr[0])
                stop_all_mariadb()
                chose_pri_ip = node_ips_arr[0]
                start_mariadb_with_wsrep(chose_pri_ip)
                start_slave_mariadb(chose_pri_ip)
                if check_mariadb_active_now(node_ips_arr[0]) and check_mariadb_active_now(node_ips_arr[1]) and check_mariadb_active_now(node_ips_arr[2]):
                   print("数据库全部启动成功。")
                else:
                   print("%s,作为集群发起人启动中"%node_ips_arr[1])
                   stop_all_mariadb()
                   chose_pri_ip = node_ips_arr[1]
                   start_mariadb_with_wsrep(chose_pri_ip)
                   start_slave_mariadb(chose_pri_ip)
                   if check_mariadb_active_now(node_ips_arr[0]) and check_mariadb_active_now(node_ips_arr[1]) and check_mariadb_active_now(node_ips_arr[2]):
                      print("数据库全部启动成功。")
                   else:
                      print("可能有文件损坏等脚本无法修复的错误，请人工参与修复")

if __name__ == "__main__":
    galera_recover()
