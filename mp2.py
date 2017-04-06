'''
ECE428: Distributed System
Machine Problem 1 -- Checkpoint 2
Author: Rui Xia, Youjie Li
Date: Feb. 25. 2017
'''

import socket
import threading
import time
import thread

#----------------------Process Node Containing ISIS Total Ordering and Heartbeat Failure Detectoin---------------
class Node(object):
    def __init__(self, host, port, port_failure, period, num_node_alive):
        ###### network parameters ####################################
        self.host = host
        self.port = port
        self.port_failure = port_failure
        ######## ISIS Total Ordering parameters ###############################
        self.pro_p = 0  # proposed priority
        self.num_node_alive = num_node_alive
        self.AGR_P = {}  # Agreed priority
        self.REC_PRO_COUNTER = {}  # Counter for received proposed priority
        self.Queue = []  # ISIS Priority Queue
        ######## Heartbeat Failure Detection paramters ########################
        self.period = period
        self.Flag_Failed = {"VM01": False,
                            "VM02": False,
                            "VM03": False,
                            "VM04": False,
                            "VM05": False,
                            "VM06": False,
                            "VM07": False,
                            "VM08": False,
                            "VM09": False,
                            "VM10": False,
                            }
        self.timestamp = {}
        self.timer_thread = {}

    def wait_input(self):  # method for take input msg
        while True:
            cmd = raw_input("")
            if cmd == 'q': # self quit process
                # self.basic_multicast("Left" + ":" + socket.gethostname())
                print "I am leaving"
                thread.interrupt_main()

            # initialization for ISIS total ordering
            self.REC_PRO_COUNTER[cmd] = 0
            self.AGR_P[cmd] = 0
            self.basic_multicast(cmd)

    def basic_multicast(self, cmd): # method for multicast msg
        for key, value in CONNECTION_LIST.iteritems():
            self.client(key, self.port, cmd)  # pack the msg as a client socket to send

    def client(self, host, port, cmd):  # method for client socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        name = CONNECTION_LIST[socket.gethostname()]  # find current machine name

        try:
            s.connect((host, port))  # connect to server
        except:
            # print host + ": Not Online" #debug
            s.close()
            return -1

        try:
            s.sendall(name + ":" + cmd)  # send message to sever
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

                self.ISIS_Total_Ordering(recv_data, addr)

            conn.close()  # close client socket

    # -------------------------------------ISIS Total Ordering-------------------------------------------
    def ISIS_Total_Ordering(self, data, addr):

        if data.split(":")[1] == "0":  # received proposed priority
            #if __debug__:
            #    print "Received Proposed Priority"

            mse = data.split(":")[-1]
            self.REC_PRO_COUNTER[mse] = self.REC_PRO_COUNTER[mse] + 1

            if float(data.split(":")[2]) > self.AGR_P[mse]: # record the maximum of proposed priority
                self.AGR_P[mse] = float(data.split(":")[2])

            if self.REC_PRO_COUNTER[mse] == self.num_node_alive: # all proposed priorities received from alive nodes
                #if __debug__:
                #    print "REC_PRO_COUNTER Done"
                # multicast agreed priority with the message by a child thread
                broadcast_AGR_P = threading.Thread(target=self.basic_multicast,
                                                   args=("1" + ":" + str(self.AGR_P[mse]) + ":" + data.split(":")[-2] + ":" + mse,))
                                                   # self.name : 1 : self.AGR_P : receive_name : message
                broadcast_AGR_P.start()

        elif data.split(":")[1] == "1":  # received agreed priority
            #if __debug__:
            #   print "Received Agreed Priority"
            # search index of agreed message in the priority self.Queue
            idx = [elem[2] for elem in self.Queue].index(data.split(":")[-2] + ":" + data.split(":")[-1])

            # mark it deliverable and update agreed priority
            self.Queue[idx][1] = True
            self.Queue[idx][0] = float(data.split(":")[2])
            self.pro_p = float(data.split(":")[2])

            # reorder priority self.Queue
            self.Queue.sort(key=lambda elem: elem[0])

            # deliver any deliverable at front of the self.Queue
            while (self.Queue and self.Queue[0][1] == True):
                print (self.Queue.pop(0)[2])

        elif data.split(":")[-1] == "failed" and self.Flag_Failed[data.split(":")[-2]] == False:  # received someone failed
            #if __debug__:
            #   print "Received failed"
            failed_machine_num = data.split(":")[-2]
            self.num_node_alive = self.num_node_alive - 1
            self.Flag_Failed[failed_machine_num] = True
            # search pending message from failed node
            failed_idx = [i for i, elem in enumerate(self.Queue) if elem[-1].split(":")[0] == failed_machine_num]

            if not failed_idx: # if no pending message from failed node
                print failed_machine_num + "failed"

            else: # has pending message

                for i in failed_idx: # kick out any non-agreed messages
                    if self.Queue[failed_idx[i]][1] == False:
                        self.Queue.pop(i)
                        failed_idx.remove(i)

                if failed_idx: # has agreed pending message
                    self.Queue.append([self.Queue[failed_idx[-1]][0] + 0.1, True, failed_machine_num + "failed"])
                    self.Queue.sort(key=lambda elem: elem[0]) # insert failed notification after the agreed one
                else:
                    print failed_machine_num + "failed"

        elif (data.split(":")[-1] != "failed"):  # received normal message
            #if __debug__:
            #    print "Received Normal Message"
            # increment proposed number
            self.pro_p = self.pro_p + 1
            # search pid
            name = CONNECTION_LIST[socket.gethostname()]
            # create proposed priority.pid
            p = float(name[-1]) / 10 + self.pro_p
            self.Queue.append([p, False, data])
            # send proposed priority back to sender by a child thread
            send_pro_p = threading.Thread(target=self.client,
                                          args=(addr[0], self.port, "0" + ":" + str(p) + ":" + data,))
                                          # self.name : 0 : prop_p : receive_name : message
            send_pro_p.start()

    #--------------------------------------Failure Detection-------------------------------------------
    def multicast_0(self):  # method for multi-cast heartbeat
        #print "Multicast Hb Entered"
        # uni-cast the msg to every node in this group
        for key, value in CONNECTION_LIST.iteritems():
            if (socket.gethostname()!= key):
                self.client_0(key, self.port_failure)  # pack the msg as a client socket to send

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

                self.timestamp[hbaddr] = time.time()*1000
                if hbaddr not in self.timer_thread:
                    self.timer_thread[hbaddr] = threading.Thread(target=self.Timer, args=(hbaddr,), kwargs={})
                    self.timer_thread[hbaddr].start()

            conn.close()  # close client socket

    def Timer(self, host):
        while True:
            time.sleep((self.period/1000)/3)
            if(time.time()*1000 > self.timestamp[host] + 2*self.period): # T+MaxOneWayDelay
                #broadcast
                self.basic_multicast(CONNECTION_LIST[host]+":"+"failed")
                return -1

#-----------------------------------Main Method----------------------------------------------
if __name__ == "__main__":
    print "ChatRoom Started ..."
    ######### global dictionary for all machine ###############################

    CONNECTION_LIST = {'sp17-cs425-g07-01.cs.illinois.edu': "VM01",
                       'sp17-cs425-g07-02.cs.illinois.edu': "VM02",
                       'sp17-cs425-g07-03.cs.illinois.edu': "VM03",
                       'sp17-cs425-g07-04.cs.illinois.edu': "VM04",
                       'sp17-cs425-g07-05.cs.illinois.edu': "VM05",
                       'sp17-cs425-g07-06.cs.illinois.edu': "VM06",
                       'sp17-cs425-g07-07.cs.illinois.edu': "VM07",
                       'sp17-cs425-g07-08.cs.illinois.edu': "VM08",
                       'sp17-cs425-g07-09.cs.illinois.edu': "VM09",
                       'sp17-cs425-g07-10.cs.illinois.edu': "VM10",

                       }

    ########################## main code ######################################
    T = 5000 # ms, period
    user_port = 9999 # port for message input
    fail_detect_port = 8888 # port for heart beat
    host = socket.gethostbyname(socket.gethostname())  # get host machine IP address
    # create process node object containing both ISIS and Failure Detection
    node = Node(host, user_port, fail_detect_port, T, len(CONNECTION_LIST))

    ###### ISIS Total Ordering Thread ###########################################
    t1 = threading.Thread(target=node.wait_input)  # thread for client (send msg)
    t2 = threading.Thread(target=node.server)  # thread for server (recv msg)

    ###### Heartbeat Threads #####################################################
    t3 = threading.Thread(target=node.heartbeating) # thread for sending heartbeating
    t4 = threading.Thread(target=node.detector) # thread for detector of heartbeating(receive heartbeat, detect failure)

    t1.daemon=True
    t2.daemon=True
    t3.daemon=True
    t4.daemon=True

    # t2.start()
    # t1.start()
    t4.start()
    t3.start()

    while True:
        pass





