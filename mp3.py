'''
ECE428: Distributed System
Machine Problem 3
Author: Rui Xia, Youjie Li
Date: April.10.2017
'''

import socket
import threading
import time
import thread
import sys
import copy

class Objects(object):
    def __init__(self):
        self.ReadLock = []
        self.WriteLock= -1
        self.value = ""

    def val_read(self):
        return self.value

    def val_write(self,value):
        self.value=value

    def setlock(self,TID,act):
        if(act=="read"):
            self.ReadLock.append(TID)
        elif(act=="write"):
            if(self.WriteLock==-1):
                self.WriteLock=TID

    def unlock(self,TID,act):
        if(act=="read"):
            self.ReadLock.pop(TID)
        elif(act=="write"):
            self.WriteLock=-1

    def getReadLock(self):
        return self.ReadLock

    def getWriteLock(self):
        return self.WriteLock

class Cat_Client(object):
    def __init__(self, host, port):
        ########## network parameters ####################################
        self.host = host
        self.port = port

        ########## client parameters #####################################
        self.tran_start=False

    def basic_multicast(self, cmd):  # method for multicast msg
        for i in range(10):
            host_remote = 'sp17-cs425-g07-' + str(i + 1).zfill(2) + '.cs.illinois.edu'
            self.client(host_remote, self.port, cmd)  # pack the msg as a client socket to send

    def wait_input(self):  # method for take input msg
        while True:
            cmd = raw_input("")

            host_tar = socket.gethostbyname(SER_CAT["Coord"])

            if len(cmd.split()) < 1:
                sys.stderr.write("invalid command\n")
            else:
                com = cmd.split(" ")[0]

                if com == "BEIGN":
                    if not self.tran_start:
                        self.tran_start=True
                        self.client(host_tar, self.port, cmd)
                    else:
                        sys.stderr.write("Invalid Command\n")

                if com == "SET":
                    if not self.tran_start:
                        sys.stderr.write("Please begin a trainsaction\n")
                    elif len(com)<3:
                        sys.stderr.write("Invalid Command\n")
                    elif com[1].split(".")[0]>'E'or com[1].split(".")[0]<'A':
                        sys.stderr.write("Invalid Server\n")
                    else:
                        self.client(host_tar, self.port,cmd)

                if com == "GET":
                    if not self.tran_start:
                        sys.stderr.write("Please begin a trainsaction\n")
                    elif len(com) != 2:
                        sys.stderr.write("Invalid Command\n")
                    elif com[1].split(".")[0]>'E'or com[1].split(".")[0]<'A':
                        sys.stderr.write("Invalid Server\n")
                    else:
                        self.client(host_tar, self.port,cmd)

                if com == "COMMIT" or com == "ABORT":
                    if not self.tran_start:
                        sys.stderr.write("Please Begin a Trainsaction\n")
                    else:
                        self.tran_start = False
                        self.client(host_tar, self.port, cmd)

                else:
                    sys.stderr.write("Invalid command\n")

                    # self.client(self.host, self.port,cmd)

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

                recv_data_list=recv_data.split(":")

                # self.Hashing(recv_data, addr)
                if recv_data_list[0] == "TID":
                    sys.stderr.write("Your TID is " + recv_data_list[1]+"\n")

                if recv_data_list[0] == "SET OK":
                    print "OK"

                elif recv_data_list[0] == "Aborted":
                    print "ABORT"
                    sys.stderr.write("Abort TID " + recv_data_list[1]+"\n")

                elif recv_data_list[0] == "Committed":
                    print "COMMIT OK"
                    sys.stderr.write("COMMIT TID " + recv_data_list[1]+"\n")

                elif recv_data_list[0] =="GOT": #"GOT:TID:cmd(SID.obj=val1 val2 val3)"
                    cmd = recv_data_list[-1]
                    TID =recv_data_list[1]
                    print cmd
                    sys.stderr.write("recived gotten value form" + TID+ "\n")

            conn.close()  # close client socket


class Cat_Coord(object):
    def __init__(self, host, port):
        ########## network parameters ####################################
        self.host = host
        self.port = port
        # self.my_node_id = int(self.host.split(".")[-1]) % (2 ** M)

        ########## Coord parameters #####################################
        self.TID=0
        self.TID_LIST={} #client_host:TID
        self.T_PAR={} # Participants for each TID; TID: [Participants' hosts]
        self.T_COM_VOTE={} #TID: [Participants hosts with vote yes]

    def multicast(self, TID,cmd):  # method for multicast msg
        for host in self.T_PAR[TID]:
            self.client(host, self.port, cmd)  # pack the msg as a client socket to send


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

                recv_data_list = recv_data.split(" ")

                com=recv_data_list[0]
                if com == "BEIGN":
                    self.TID= self.TID+1
                    self.TID_LIST[addr[0]] = self.TID
                    self.T_PAR[self.TID] = []

                    return_thr = threading.Thread(target=self.client,
                                                  args=(addr[0], self.port,
                                                        "TID:"+str(self.TID)),
                                                  # return:nodeid*returnvalue:timestamp
                                                  kwargs={})
                    # return_thr.daemon = True
                    return_thr.start()

                    sys.stderr.write("Received begin and finish allocate TID\n")

                elif com == "SET":
                    TID =self.TID_LIST[addr[0]]
                    target = recv_data_list[1].split(".")[0] #ABCDE
                    obj=recv_data_list[1].split(".")[1]

                    host_tar=socket.gethostbyname(SER_CAT[target])
                    self.T_PAR[TID].append(host_tar)

                    send_thr = threading.Thread(target=self.client,
                                                  args=(host_tar, self.port,
                                                        TID+":SET:"+obj+":"+" ".join(recv_data_list[2:])),
                                                  # Send for target(A~E) = TID:SET:x:1 2 3
                                                  kwargs={})
                    send_thr.start()

                    return_thr = threading.Thread(target=self.client,
                                                  args=(addr[0], self.port,
                                                        "SET OK"),
                                                  # return:set ok
                                                  kwargs={})
                    return_thr.start()

                    sys.stderr.write("Received SET and send to corresponding server\n")

                elif com == "GET":
                    TID = self.TID_LIST[addr[0]]
                    target = recv_data_list[1].split(".")[0]
                    obj = recv_data_list[1].split(".")[1]

                    host_tar = socket.gethostbyname(SER_CAT[target])
                    self.T_PAR[TID].append(host_tar)

                    send_thr = threading.Thread(target=self.client,
                                                args=(host_tar, self.port,
                                                      TID + ":GET:" + obj),
                                                # Send for target(A~E)= TID:GET:x
                                                kwargs={})
                    send_thr.start()

                    sys.stderr.write("Received GET and send to corresponding server\n")

                elif com == "COMMIT":
                    TID =self.TID_LIST[addr[0]]
                    self.T_COM_VOTE[TID] = []

                    multicast_thr = threading.Thread(target=self.multicast,
                                                     args=(TID, self.port,
                                                           TID+":"+ "canCommit?"),
                                                     # Send for target TID:canCommit?
                                                     kwargs={})
                    multicast_thr.start()
                    sys.stderr.write("Received "+ com+ " and multicast canCommit?\n")

                elif com=="VOTE": #VODE TID serverID
                    TID=recv_data_list[1]
                    SID=recv_data_list[-1]

                    sys.stderr.write("Received votes from "+SID+" for "+TID+"\n")

                    self.T_COM_VOTE[TID].append(SID)

                    if len(self.T_COM_VOTE[TID])==len(self.T_PAR[TID]):
                        multicast_thr = threading.Thread(target=self.multicast,
                                                         args=(TID, self.port,
                                                               TID + ":" + "doCommit"),
                                                         # Send for target TID:doCommit
                                                         kwargs={})
                        multicast_thr.start()

                        return_thr = threading.Thread(target=self.client,
                                                      args=(addr[0], self.port,
                                                            "Committed:"+TID),

                                                      kwargs={})
                        return_thr.start()
                        sys.stderr.write("Received all votes and multicast doCommit\n")

                elif com == "ABORT":
                    TID = self.TID_LIST[addr[0]]
                    self.T_COM_VOTE[TID] = []

                    multicast_thr = threading.Thread(target=self.multicast,
                                                     args=(TID, self.port,
                                                           TID + ":" + "ABORT"),
                                                     # Send for target TID:ABORT
                                                     kwargs={})
                    multicast_thr.start()
                    sys.stderr.write("Received " + com + " and multicast ABORT\n")
                    return_thr = threading.Thread(target=self.client,
                                                  args=(addr[0], self.port,
                                                        "Aborted:" + TID),

                                                  kwargs={})
                    return_thr.start()

                elif com=="GOTTEN": #GOTTEN TID SID.obj=value
                    TID=recv_data_list[1]
                    cmd=" ".join(recv_data_list[2:])
                    host_tar = self.T_PAR[TID]
                    send_thr = threading.Thread(target=self.client,
                                                args=(host_tar, self.port,
                                                "GOT:"+TID+":"+cmd),
                                                # send for client "GOT:TID:cmd(SID.obj=val1 val2 val3)"
                                                kwargs={})
                    send_thr.start()
                    sys.stderr.write("Received " + recv_data + " and send back to client\n")

                else:
                    sys.stderr.write("Coord received wrong msg"+ recv_data+"\n")

            conn.close()  # close client socket

class Cat_Server(object):
    def __init__(self, host, port):
        ########## network parameters ####################################
        self.host = host
        self.port = port


        ########## Server parameters #####################################


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

                recv_data_list = recv_data.split(":")
                com = recv_data_list[1]
                if com=="SET":
                    print recv_data

                elif com=="GET": #TID:GET:obj
                    print recv_data
                    TID=recv_data_list[0]
                    obj=recv_data_list[-1]
                    SID=CAT[socket.gethostname()].split("_")[-1]
                    return_thr = threading.Thread(target=self.client,
                                                  args=(addr[0], self.port,
                                                        "GOTTEN "+TID+" "+ SID+"."+obj+"="+"unkown"),
                                                        #GOTTEN TID SID.obj=value
                                                  kwargs={})
                    return_thr.start()

                elif com=="canCommit?":
                    print recv_data

                    return_thr = threading.Thread(target=self.client,
                                                  args=(addr[0], self.port,
                                                        "VOTE " + recv_data_list[0] + " "+CAT[socket.gethostname()].split("-")[-1]),
                                                        #Vote TID SID
                                                  kwargs={})
                    return_thr.start()

                elif com=="doCommit":
                    print recv_data


                elif com=="ABORT":
                    print recv_data
                else:
                    print "server received worong msg "+ recv_data

            conn.close()  # close client socket










#  ############################ main code #######################################
if __name__ == "__main__":
    user_port = 9999  # port for message input
    host = socket.gethostbyname(socket.gethostname())
    CAT = {"sp17-cs425-g07-01.cs.illinois.edu": "Server_A",
           "sp17-cs425-g07-02.cs.illinois.edu": "Server_B",
           "sp17-cs425-g07-03.cs.illinois.edu": "Server_C",
           "sp17-cs425-g07-04.cs.illinois.edu": "Server_D",
           "sp17-cs425-g07-05.cs.illinois.edu": "Server_E",
           "sp17-cs425-g07-06.cs.illinois.edu": "Coord_0",
           "sp17-cs425-g07-07.cs.illinois.edu": "Client_1",
           "sp17-cs425-g07-08.cs.illinois.edu": "Client_2",
           "sp17-cs425-g07-09.cs.illinois.edu": "Client_3"}

    SER_CAT = {"A": "sp17-cs425-g07-01.cs.illinois.edu",
               "B": "sp17-cs425-g07-02.cs.illinois.edu",
               "C": "sp17-cs425-g07-03.cs.illinois.edu",
               "D": "sp17-cs425-g07-04.cs.illinois.edu",
               "E": "sp17-cs425-g07-05.cs.illinois.edu",
               "Coord": "sp17-cs425-g07-06.cs.illinois.edu"}

    # launch different classes according to its domain name
    catog = CAT[socket.gethostname()].split("_")
    user_port = 9999  # port for message input


    if catog[0] == "Server":
        print "_".join(catog) + " Started"
        cat_node = Cat_Server(host, user_port)
        t1 = threading.Thread(target=cat_node.server)
        t1.daemon=True
        t1.start()

    elif catog[0] == "Coord":
        print "_".join(catog) + " Started"
        cat_node = Cat_Coord(host, user_port)
        t1 = threading.Thread(target=cat_node.server)
        t1.daemon = True
        t1.start()

    elif catog[0] == "Client":
        print "_".join(catog) + " Started"
        cat_node = Cat_Client(host, user_port)
        t1 = threading.Thread(target=cat_node.wait_input)  # thread for client (send msg)
        t2 = threading.Thread(target=cat_node.server)
        t1.daemon=True
        t2.daemon=True
        t2.start()
        t1.start()
    else:
        print "CAT Error"
        exit(0)


    while True:
        pass






