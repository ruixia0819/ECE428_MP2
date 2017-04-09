'''
ECE428: Distributed System
Machine Problem 2
Author: Rui Xia, Youjie Li
Date: April.8. 2017
'''

import socket
import threading
import time
import thread
import sys
import copy

class Node(object):
    def __init__(self, host, port, port_failure, period, num_node_alive):
        ########## network parameters ####################################
        self.host = host
        self.port = port
        self.port_failure = port_failure
        ######## Heartbeat Failure Detection paramters ########################
        self.period = period
        self.timestamp = {}
        self.timer_thread = {}
        ######## Hashing paramters ########################
        self.local_memory={}
        self.owner=[]
        self.NODE_ID_LIST = {}  #node_id: ip addr
        self.return_value = {}
        self.sec_fail=-1
        self.recovering = False
        self.rebalancing = False
        self.sec_flag = False
        # initialize node id list with local id
        self.my_vm_id= socket.gethostname().split(".")[0].split("-")[-1]
        self.my_node_id = int(self.host.split(".")[-1]) % (2 ** M)
        self.NODE_ID_LIST[self.my_node_id] = socket.gethostname()
        sys.stderr.write("My Node ID is "+str(self.my_node_id)+'\n')

    def basic_multicast(self, cmd):  # method for multicast msg
        for i in range(10):
            host_remote='sp17-cs425-g07-'+str(i+1).zfill(2)+'.cs.illinois.edu'
            self.client(host_remote, self.port, cmd)  # pack the msg as a client socket to send

    def wait_input(self):  # method for take input msg
        while True:

            cmd = raw_input("")
            # if cmd== "send self":
            #     self.client(self.NODE_ID_LIST[self.my_node_id], self.port, "search:" + "x" )
            #     time.sleep(10)
            #     print self.return_value

            if len(cmd.split())<1:
                sys.stderr.write("invalid command\n")
            else:
                if cmd.split()[0] == "BATCH":# batch command
                    if len(cmd.split())!=3:
                        sys.stderr.write("invalid command\n")
                    else:
                        file2 = open(cmd.split()[2], 'w')
                        stdout = sys.stdout
                        sys.stdout = file2

                        try:
                            file1=open(cmd.split()[1], "r")
                        except:
                            sys.stderr.write("file not found\n")

                        for line in file1:
                            self.get_command(line)

                        file1.close()
                        sys.stdout = stdout
                        file2.close()

                        try:
                            sys.stderr.write(" print file2\n")
                            with open(cmd.split()[2], "r") as f:
                                for line in f:
                                    sys.stderr.write(line + '\n')
                        except:
                            sys.stderr.write("create file2 failed")
                else:
                    self.get_command(cmd)


                #self.client(self.host, self.port,cmd)

    def client(self, host, port, cmd):  # method for client socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # name = CONNECTION_LIST[socket.gethostname()]  # find current machine name
        try:
            s.connect((host, port))  # connect to server
        except:
            # print host + ": Not Online" #debug
            s.close()
            return -1

        try:
            s.sendall(cmd)  # send message to sever
        except:
            s.close()
            return -1

        s.close()
        return 0

    def server(self):
        ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ss.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ss.bind((self.host, self.port))
        ss.listen(100)

        while True:
            conn, addr = ss.accept()
            # print 'Connected by ', addr

            while True:
                recv_data = conn.recv(1024)
                if not recv_data:  # recv ending msg from client
                    break

                # self.Hashing(recv_data, addr)
                if (recv_data.split(":")[0] == "store"):
                    self.local_memory[recv_data.split(":")[1]]=recv_data.split(":")[2]

                elif(recv_data.split(":")[0] == "search"):
                    key_search = recv_data.split(":")[1]
                    if key_search in self.local_memory.keys():
                        tt=str(round(time.time() * 1000) % (2**15))
                        return_thr = threading.Thread(target=self.client,
                                                      args=(addr[0],self.port,
                                                        "return:"+str(self.my_vm_id)+"*"
                                                        +self.local_memory[key_search]
                                                        +":"+tt),
                                                      #return:nodeid*returnvalue:timestamp
                                                      kwargs={})
                        return_thr.start()

                elif(recv_data.split(":")[0] == "return"):
                    self.return_value[recv_data.split(":")[-1]]=recv_data.split(":")[1]


                elif (recv_data.split(":")[-1] == "failed"):  # received failed message

                    #recover
                    key_failed=""
                    for key, value in self.NODE_ID_LIST.iteritems():
                        if value==recv_data.split(":")[0]:
                            key_failed= key
                    if key_failed==self.my_node_id:
                        continue

                    try:
                        del self.NODE_ID_LIST [key_failed]

                        if not self.recovering:
                            t_wsf = threading.Thread(target=self.Timer_wsf, args=(key_failed,))
                            t_wsf.start()
                        else:
                            self.sec_fail = key_failed
                    except:
                        pass

                    sys.stderr.write(recv_data+'\n')

            conn.close()  # close client socket

    def Timer_wsf(self,first_fail): #wait second failure
        sys.stderr.write("start recovery\n")
        self.rebalancing = True #start rebalance after recover completed
        self.recovering=True
        start_len=len(self.NODE_ID_LIST)
        for i in range(10):
            time.sleep(self.period/1000) #T(s)
            sys.stderr.write("start_len="+str(start_len)+'\n')
            sys.stderr.write("curr_len="+str(len(self.NODE_ID_LIST))+'\n')

            if len(self.NODE_ID_LIST)<start_len:
                sys.stderr.write( "sec_fail"+str(self.sec_fail)+'\n')
                self.recover(self.sec_fail)
                sys.stderr.write("sec_fail recovered\n")
                break  # after T+MaxOneWayDelay
        sys.stderr.write( "first_fail" + str(first_fail)+'\n')
        self.recover(first_fail)
        self.recovering = False
        self.rebalancing =False
        sys.stderr.write( "recovery completed"+'\n')

        return -1




# --------------------------------------key-value storage---------------------------------------------

    def recover(self,node_fail_id):
        local_mem=copy.deepcopy(self.local_memory)

        sorted_node_id = sorted(self.NODE_ID_LIST.keys())
        suc_id = sorted_node_id[0]
        suc_idx = 0
        for idx, node_id in enumerate(sorted_node_id):

            if node_id >= node_fail_id:
                suc_id = node_id
                suc_idx = idx
                break
        pre_idx=suc_idx-1
        pre_id=sorted_node_id[pre_idx]

        if suc_idx == len(sorted_node_id)-1:
            suc_idx = -1


        if self.my_node_id==suc_id:

            for key in local_mem:
                key_id=ord(key[0]) % (2 ** M)
                if suc_id>node_fail_id:
                    if key_id<=suc_id and key_id>node_fail_id:
                        self.client(self.NODE_ID_LIST[pre_id], self.port, "store:" + key + ":" + local_mem[key])
                        sys.stderr.write( str(self.my_node_id)+" have stored "+key+ " in precessor " +str(pre_id)+'\n')
                else:
                    if (key_id>node_fail_id and key_id<2**M) or (key_id>=0 and  key_id<=suc_id):
                        self.client(self.NODE_ID_LIST[pre_id], self.port, "store:" + key + ":" + local_mem[key])
                        sys.stderr.write( str(self.my_node_id)+" have stored "+key+ " in precessor " +str(pre_id)+'\n')
              
        elif self.my_node_id==pre_id:
            for key in local_mem:
                key_id=ord(key[0]) % (2 ** M)
                if node_fail_id>pre_id:
                    if key_id>pre_id and key_id<=node_fail_id:
                        self.client(self.NODE_ID_LIST[sorted_node_id[suc_idx+1]], self.port, "store:" + key + ":" + local_mem[key])
                        sys.stderr.write( str(self.my_node_id)+" have stored "+key+" in suc.suc "+str(sorted_node_id[suc_idx+1])+'\n')

                else:
                    if (key_id>pre_id and key_id<2**M) or( key_id>=0 and key_id<=node_fail_id):
                        self.client(self.NODE_ID_LIST[sorted_node_id[suc_idx+1]], self.port, "store:" + key + ":" + local_mem[key])
                        sys.stderr.write( str(self.my_node_id)+" have stored "+key+" in suc.suc "+str(sorted_node_id[suc_idx+1])+'\n')

                if node_fail_id>sorted_node_id[pre_idx-1]:
                    if key_id<=node_fail_id and key_id>sorted_node_id[pre_idx-1]:
                        self.client(self.NODE_ID_LIST[suc_id], self.port,"store:" + key + ":" + local_mem[key])
                        sys.stderr.write( str(self.my_node_id)+" have stored "+key +"in suc " +str(suc_id)+'\n')
                else:
                    if (key_id<=node_fail_id and key_id>0) or  (key_id>sorted_node_id[pre_idx-1] and key_id<2**M):
                        self.client(self.NODE_ID_LIST[suc_id], self.port,"store:" + key + ":" + local_mem[key])
                        sys.stderr.write( str(self.my_node_id)+" have stored "+key +"in suc " +str(suc_id)+'\n')

    def rebalance(self,node_new_id):
        local_mem = copy.deepcopy(self.local_memory)
        sys.stderr.write("rebalance started \n")
        self.rebalancing=True
        sorted_node_id = sorted(self.NODE_ID_LIST.keys())
        suc_idx = 0
        for idx, node_id in enumerate(sorted_node_id):
            if node_id > node_new_id:
                suc_idx = idx
                break
        pre_idx = suc_idx - 2
        pre_pre_idx= pre_idx -1

        suc_id= sorted_node_id[suc_idx]
        pre_id = sorted_node_id[pre_idx]
        pre_pre_id=sorted_node_id[pre_pre_idx]

        if suc_idx == len(sorted_node_id)-1:
            suc_idx=-1

        suc_suc_idx= suc_idx+1
        suc_suc_id=sorted_node_id[suc_suc_idx]


        if self.my_node_id==suc_id:
            for key in local_mem:
                key_id = ord(key[0]) % (2 ** M)

                if suc_suc_id>suc_id:
                    if not (key_id<=suc_suc_id and key_id>suc_id):
                        self.client(self.NODE_ID_LIST[node_new_id], self.port, "store:" + key + ":" + local_mem[key])
                        sys.stderr.write( str(self.my_node_id)+" have stored "+key+ " in r " +str(node_new_id)+'\n')
                else:
                    if not((key_id>suc_id and key_id<2**M) or (key_id>=0 and  key_id<=suc_suc_id)):
                        self.client(self.NODE_ID_LIST[node_new_id], self.port, "store:" + key + ":" + local_mem[key])
                        sys.stderr.write( str(self.my_node_id)+" have stored "+key+ " in  " +str(node_new_id)+'\n')


                if pre_id>pre_pre_id:
                    if(key_id<=pre_id and key_id>pre_pre_id):
                        del self.local_memory[key]
                else:
                    if(key_id > pre_pre_id and key_id < 2 ** M) or (key_id >= 0 and key_id <= pre_id):
                        del self.local_memory[key]


        if self.my_node_id==pre_id:
            for key in local_mem:
                key_id = ord(key[0]) % (2 ** M)
                if suc_id > node_new_id:
                    if (key_id <= suc_id and key_id > node_new_id):
                        del self.local_memory[key]
                else:
                    if (key_id > node_new_id and key_id < 2 ** M) or (key_id >= 0 and key_id <= suc_id):
                        del self.local_memory[key]


        if self.my_node_id == suc_suc_id:
            for key in local_mem:
                key_id = ord(key[0]) % (2 ** M)
                if node_new_id > pre_id:
                    if (key_id <= node_new_id and key_id > pre_id):
                        del self.local_memory[key]
                else:
                    if (key_id > pre_id and key_id < 2 ** M) or (key_id >= 0 and key_id <= node_new_id):
                        del self.local_memory[key]

        self.rebalancing = False
        sys.stderr.write("rebalance completed\n")

        return 0

    def get_command(self, data):
        cmd=data.split()
        len_cmd =len(data.split())

        if len_cmd<1:
            sys.stderr.write("empty command \n")

        else:
            if cmd[0] == "SET":  # set command
                if len_cmd<=1:
                    sys.stderr.write("invalid SET\n")
                else:
                    if len_cmd==2:
                        value_set=" "

                    else:
                        value_set=" ".join(cmd[2:])

                    self.com_set(cmd[1],value_set)
                    print "SET OK"


            elif cmd[0] == "GET":  # get command
                if len_cmd !=2:
                    sys.stderr.write("invalid GET\n")
                else:
                    self.com_get(cmd[1])

            elif cmd[0] == "OWNERS":  # owners command
                if len_cmd != 2:
                    sys.stderr.write("invalid OWNERS\n")
                else:
                    self.com_owner(cmd[1])

            elif cmd[0] == "LIST_LOCAL":  # get command
                if len_cmd != 1:
                    sys.stderr.write("invalid Command\n")
                else:
                    self.com_list()
            else:
                sys.stderr.write("Invalid Command\n")

    def com_set(self,key_input, value_input):
        key_id=ord(key_input[0])%(2**M)
        sorted_node_id=sorted(self.NODE_ID_LIST.keys())

        store_id = sorted_node_id[0]
        store_idx = 0
        for idx, node_id in enumerate(sorted_node_id):
            if node_id >= key_id:
                store_id = node_id
                store_idx = idx
                break

        self.client(self.NODE_ID_LIST[store_id], self.port, "store:" + key_input + ":" + value_input)
        idx_pre = store_idx - 1
        self.client(self.NODE_ID_LIST[sorted_node_id[idx_pre]], self.port, "store:" + key_input + ":" + value_input)
        idx_suc = (store_idx + 1) % len(sorted_node_id)
        self.client(self.NODE_ID_LIST[sorted_node_id[idx_suc]], self.port, "store:" + key_input + ":" + value_input)

    def com_get(self,key_input):
        self.return_value={}
        key_id = ord(key_input[0]) % (2 ** M)
        sorted_node_id = sorted(self.NODE_ID_LIST.keys())

        store_id=sorted_node_id[0]
        store_idx=0
        for idx, node_id in enumerate(sorted_node_id):
            if node_id >= key_id:
                store_id= node_id
                store_idx=idx
                break

        self.client(self.NODE_ID_LIST[store_id], self.port, "search:" + key_input)
        idx_pre=store_idx-1
        self.client(self.NODE_ID_LIST[sorted_node_id[idx_pre]], self.port, "search:" + key_input)
        idx_suc = (store_idx + 1) % len(sorted_node_id)
        self.client(self.NODE_ID_LIST[sorted_node_id[idx_suc]], self.port, "search:" + key_input)

        time.sleep(T/5/1000) #timeout=T/5
        if len(self.return_value)>0:
            value_return=" ".join(self.return_value[sorted(self.return_value.keys())[-1]].split("*")[1:])
            print "Found"+": "+value_return #returnvalue
            # sys.stderr.write("\n".join(self.return_value.keys()))
            # sys.stderr.write(self.return_value)
        else:
            print "Not found"

    def com_list(self):
        local_mem = copy.deepcopy(self.local_memory)
        for key, value in local_mem.iteritems():
            print key + ":" + value
        print "END LIST"

    def com_owner(self,key_input):
        self.return_value={}
        self.owner=[]
        owner_cal=[]

        key_id = ord(key_input[0]) % (2 ** M)
        sorted_node_id = sorted(self.NODE_ID_LIST.keys())

        store_id = sorted_node_id[0]
        store_idx = 0
        for idx, node_id in enumerate(sorted_node_id):
            if node_id >= key_id:
                store_id = node_id
                store_idx = idx
                break

        owner_cal.append(self.NODE_ID_LIST[store_id].split(".")[0].split("-")[-1])
        idx_pre = store_idx - 1
        owner_cal.append(self.NODE_ID_LIST[sorted_node_id[idx_pre]].split(".")[0].split("-")[-1])
        idx_suc = (store_idx + 1) % len(sorted_node_id)
        owner_cal.append(self.NODE_ID_LIST[sorted_node_id[idx_suc]].split(".")[0].split("-")[-1])

        sys.stderr.write("possible owers:\n")
        sys.stderr.write(" ".join(owner_cal)+'\n')

        self.client(self.NODE_ID_LIST[store_id], self.port, "search:" + key_input)
        self.client(self.NODE_ID_LIST[sorted_node_id[idx_pre]], self.port, "search:" + key_input)
        self.client(self.NODE_ID_LIST[sorted_node_id[idx_suc]], self.port, "search:" + key_input)

        for i in range(5):
            time.sleep(float(self.period)/1000/25)  # timeout=T/5
            if len(self.return_value) >= 3:
                sys.stderr.write(str(len(self.return_value))+'\n')
                break

        if len(self.return_value)<1:
            print "Owners Not found"
        else:
            sys.stderr.write(str(len(self.return_value)) + '\n')
            for key, value in self.return_value.iteritems():
                self.owner.append(value.split("*")[0])
            print " ".join(self.owner)




#--------------------------------------Failure Detection-------------------------------------------
    def multicast_0(self):  # method for multi-cast heartbeat
        #print "Multicast Hb Entered"
        # uni-cast the msg to every node in this group
        for i in range(10):
            host_remote='sp17-cs425-g07-'+str(i+1).zfill(2)+'.cs.illinois.edu'
            if socket.gethostname()!= host_remote:
                self.client_0(host_remote, self.port_failure)  # pack the msg as a client socket to send

    def client_0(self, host, port):  # method for heartbeat client socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((host, port))  # connect to server
        except:
            s.close()
            return -1

        try:
            s.sendall(socket.gethostname())  # send message to sever
        except:
            s.close()
            return -1

        s.close()
        return 0

    def heartbeating(self): # Heartbeat main method
        prev_time = time.time()*1000
        while True:
            time.sleep((self.period/1000)/10) # delay for checking
            cur_time = time.time()*1000
            if(cur_time-prev_time>self.period): #send heartbeating every period
                prev_time = cur_time
                self.multicast_0()

    def detector(self): # Heartbeat Detector: receive, check, multicast failure flag
        ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ss.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ss.bind((self.host, self.port_failure))
        ss.listen(100)
        while True:
            conn, addr = ss.accept()
            while True:
                hbaddr = conn.recv(1024)
                if not hbaddr:  # recv ending msg from client
                    break

                if(hbaddr not in self.NODE_ID_LIST.values()):
                    # rebalance
                    if not self.rebalancing:
                        new_id = int(socket.gethostbyname(hbaddr).split(".")[-1]) % (2 ** M)
                        self.NODE_ID_LIST[new_id] = hbaddr

                        if hbaddr not in self.timer_thread:
                            self.timestamp[hbaddr] = time.time() * 1000
                            self.timer_thread[hbaddr] = threading.Thread(target=self.Timer, args=(hbaddr,), kwargs={})
                            self.timer_thread[hbaddr].start()

                        if len(self.local_memory)!=0:
                            t_reb = threading.Thread(target=self.rebalance, args=(new_id,))
                            t_reb.start()
                        else:
                            sys.stderr.write("node "+str(new_id)+" connected\n")
                else:
                    #if hbaddr in self.timer_thread:
                    self.timestamp[hbaddr] = time.time() * 1000

            conn.close()  # close client socket

    def Timer(self, host):
        while True:
            time.sleep((self.period / 1000) / 3)
            if (time.time() * 1000 > self.timestamp[host] + 2 * self.period):  # T+MaxOneWayDelay
                # broadcast
                self.basic_multicast(host + ":" + "failed")
                return -1

#-----------------------------------Main Method-----------------------------------------------
if __name__ == "__main__":
    print "Started ..."

    ############################ main code #######################################
    M = 5  # hashing bits
    T = 2500 # ms, period
    user_port = 9999 # port for message input
    fail_detect_port = 8888 # port for heart beat
    host = socket.gethostbyname(socket.gethostname())  # get host machine IP address
    # create process node object containing both ISIS and Failure Detection

    node = Node(host, user_port, fail_detect_port, T, 10)


    ###### ISIS Total Ordering Thread ###########################################
    t1 = threading.Thread(target=node.wait_input)  # thread for client (send msg)
    t2 = threading.Thread(target=node.server)  # thread for server (recv msg)

    ###### Heartbeat Threads #####################################################
    t3 = threading.Thread(target=node.heartbeating) # thread for sending heartbeating
    t4 = threading.Thread(target=node.detector) # thread for detector of heartbeating(receive heartbeat, detect failure)
    #
    t1.daemon=True
    t2.daemon=True
    t3.daemon=True
    t4.daemon=True

    t2.start()
    t1.start()
    t4.start()
    t3.start()

    while True:
        pass





