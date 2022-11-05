# reimplementation of MP2, in this implementation, we assume master will always join first and will never leave the ring

import socket
import json
import time
import threading
import sys

MASTER_PORT = 3333
PING_PORT = [3345 + i for i in range(10)]
ML_lock = threading.Lock()
TS_lock = threading.Lock()

class Server:
    def __init__(self, master_host="fa22-cs425-2210.cs.illinois.edu"):
        self.hostname = socket.gethostname()
        self.hostID = int(self.hostname[13:15])
        print("I am VM ", self.hostID)
        self.master_host = master_host
        self.is_master = True if self.hostname == master_host else False
        if self.is_master:
            print("I am the master node -^.^- ")
        else:
            print("I am a pariah node :(")
        self.ML = []
        self.timer = time
        # self.neighbor_timestamps = {} # dict of key = hostname, value = [isInRing, timestamp], only used in master node to record a node status
        self.init_timestamps()
    
    def init_timestamps(self):
        # initialize self.neighbor_timestamps
        with TS_lock:
            for i in range(1, 11):
                host = "fa22-cs425-22%02d.cs.illinois.edu" % i
                # self.neighbor_timestamps[host] = [0, 0]

    def get_neighbors(self):
        # If a node is not in the ring, self.ML = [] hence directly return []
        # if a node is in the ring, find the index of it and return its 3 or less successors
        if not len(self.ML):
            return []
        i = 0
        while i < len(self.ML) and (self.ML[i] != self.hostname):
            i += 1
        if i == len(self.ML) and (i != 0):
            raise NotImplementedError("TODO: fix bug that current server is in the ring but not in the membership list!")
        if len(self.ML) <= 4:
            ret = self.ML[:i] + self.ML[i+1:]
        else:
            ret = (self.ML * 2)[i+1:i+4]
        return ret
    
    def join(self):
        if self.hostname == self.master_host:
            # master node must join first
            self.ML.append(self.hostname)
        else:
            # a common node join, do nothing but send a join request ["join", current node hostname] to master
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.master_host, MASTER_PORT))
            s.send(json.dumps(["join", self.hostname]).encode())
            s.close()
    
    def leave(self, host=None):
        if host == None:
            # self.leave() leave the node itself from the ring
            host = self.hostname
        # TODO: send a leave message to the master
        if host == self.master_host:
            # we do not allow master to leave
            print("Cannot force master node to leave!")
            return
        # else send leave message to the master
        if host == self.hostname:
            # self leave, remember to clear self.ML
            self.ML = []
        # node leave, do nothing but send a leave request ["leave", host] to master
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.master_host, MASTER_PORT))
        s.send(json.dumps(["leave", host]).encode())
        s.close()

    def listen_to_master(self):
        if self.is_master:
            # master no need to listen to itself, just directly manipulate is fine
            # in this case, it is convenient for us to only use one port for both send to and listen from master for each node 
            return
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.hostname, MASTER_PORT))
        s.listen(5)
        while 1:
            conn, addr = s.accept()
            info = conn.recv(4096).decode()
            if info == "you are dead":
                self.ML = []
            else:
                # simply update whatever heard from the master
                self.ML = json.loads(info)

    def listen_join_and_leave(self):
        if not self.is_master:
            # we only handle leave and join in master node
            return
        #TODO: run a thread to know who are joining and leaving
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.hostname, MASTER_PORT))
        s.listen(5)
        while 1:
            conn, addr = s.accept()
            # leave or join message heard from common nodes ["join"/"leave", node hostname]
            news = json.loads(conn.recv(4096).decode())
            
            if news[0] == "leave":
                idx = -1
                for i in range(len(self.ML)):
                    if self.ML[i] == news[1]:
                         idx = i
                         break
                if idx == -1:
                    print(f"node {news[1]} already left")
                    # This should indicate that multiple nodes detected the leave/fail and reported to the master, we do nothing
                    # raise NotImplementedError("TODO: fix bug in listen_join_and_leave that inactive host is not in the membership list")
                    continue
                # directly remove the node from self.ML
                self.ML = self.ML[:idx] + self.ML[idx+1:]
                # with TS_lock:
                #     self.neighbor_timestamps[news[1]][0] = 0
            elif news[0] == "join":
                if news[1] in self.ML:
                    print("Node already exists in the ring!")
                    continue
                # only a node join, mark its existance and timestamp and add to self.ML
                new_t = threading.Thread(target=self.receive_ack, args=news[1])
                new_t.start()
                # with TS_lock:
                    # self.neighbor_timestamps[news[1]][0] = 1
                    # self.neighbor_timestamps[news[1]][1] = self.timer.time()
                self.ML.append(news[1])
                # print("master send join messages: ", self.ML)
            else:
                raise NotImplementedError("TODO: fix bug in listen_join_and_leave, the received news has unrecognizable message header")
            # TODO: ask everyone to update their membership list
            for host in self.ML:
                if host == self.master_host:
                    continue
                # broadcast the updated ML to every node marked in the ring
                s1 = socket.socket()
                s1.connect((host, MASTER_PORT))
                s1.send(json.dumps(self.ML).encode())
                s1.close()
                # print("successfully send ML: ", self.ML)
    
    def ping(self):
        if self.is_master:
            return
        # send ping to check neighbors alive every 300 ms
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((self.hostname, PING_PORT[self.hostID]))
        while 1:
            time.sleep(1)
            s.sendto(self.hostname.encode(), (self.master_host, PING_PORT[self.hostID]))

    def receive_ack(self, monitor_host):
        if not self.is_master:
            return
        monitorID = int(monitor_host[13:15])
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((self.hostname, PING_PORT[monitorID]))
        s.settimeout(3)
        while(1):
            try:
                s.recvfrom(4096)
            except:
                print("Host " + str(monitorID) + " Fail")
                try:
                    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s1.connect((monitor_host, MASTER_PORT))
                    s1.send("you are dead".encode())
                    s1.close()
                except:
                    self.leave(monitor_host)
                return
        
        

    def shell(self):
        while 1:
            print("Please input command:")
            command = input()
            if command == "list_mem":
                with ML_lock:
                    print(self.ML)
            elif command == "list_nb":
                print(self.get_neighbors())
            elif command == "join":
                self.join()
            elif command == "leave":
                self.leave()
            elif command == "help":
                print("=======================================================")
                print("1. list_mem: list current membership list in the ring")
                print("2. list_nb: list current neighbor list in current node")
                print("3. join: join current node to the ring")
                print("4. leave: leave current node from the ring")
                print("5. help: show this usage prompt")
                print("6. exit: shutdown this node")
                print("=======================================================")
            else:
                print("invalid command, use help to check available commands!")
    
    def run(self):
        tm = threading.Thread(target=self.listen_join_and_leave, name="listen_join_and_leave")
        tn = threading.Thread(target=self.listen_to_master, name="listen_to_master")
        t1 = threading.Thread(target=self.shell, name="shell")
        t2 = threading.Thread(target=self.ping, name="ping")
        # t3 = [threading.Thread(target=self.receive_ack, name=f"receive_ack {i}", args=[i]) for i in range(10)]
        # t4 = threading.Thread(target=self.check_neighbors_alive, name="check_neighbors_alive")

        tm.start()
        tn.start()
        t1.start()
        t2.start()
        # for i in range(10):
        #     t3[i].start()
        # t4.start()
        print('all threads started!')
        tm.join()
        print('tm join')
        tn.join()
        print('tn join')
        t1.join()
        print('t1 join')
        t2.join()
        print('t2 join')
        # for i in range(10):
        #     t3[i].join()
        print('t3 join')
        # t4.join()
        print('t4 join')

# s = Server()
# s.run()
# print("finish!")
