# reimplementation of MP2, in this implementation, we assume master will always join first and will never leave the ring

import socket
import json
import time
import threading
import sys

MASTER_PORT = 2333
PING_PORT = 2345
ACK_PORT = 2346
RECEIVE_ACK_PORT = 2347
EXIT_SIGNAL = 0
MONITOR_PORT = 3456
ML_lock = threading.Lock()

class Server:
    def __init__(self, master_host="fa22-cs425-2210.cs.illinois.edu"):
        self.hostname = socket.gethostname()
        self.master_host = master_host
        print(self.master_host, self.hostname)
        if self.hostname == self.master_host:
            print("I am the master node -^.^- ")
        else:
            print("I am a pariah node :(")
        self.IP = socket.gethostbyname(self.hostname)
        self.ML = []
        self.online = False
        self.neighbor_timestamps = {} # dict of key = hostname, value = [isNeighbor, timestamp] 
        self.init_timestamps()
    
    def get_ML(self):
        return self.ML

    def is_master(self):
        if self.hostname == self.master_host:
            return True
        else:
            return False
    
    def init_timestamps(self):
        for i in range(1, 11):
            host = "fa22-cs425-22%02d.cs.illinois.edu" % i
            self.neighbor_timestamps[host] = [0, time.time()]

    def get_neighbors(self):
        if not self.online:
            return []
        # ML_lock.acquire()
        if not len(self.ML):
            # haven't join in the ring 
            # ML_lock.release()
            return []
        i = 0
        while i < len(self.ML) and (self.ML[i] != self.hostname):
            # print(self.ML[i])
            i += 1
        if i == len(self.ML) and (i != 0):
            # ML_lock.release()
            raise NotImplementedError("TODO: fix bug that current server is in the ring but not in the membership list!")
        if len(self.ML) <= 4:
            ret = self.ML[:i] + self.ML[i+1:]
        else:
            ret = (self.ML * 2)[i+1:i+4]
        # ML_lock.release()
        return ret
    
    def join(self):
        self.online = True
        # self.init_timestamps()
        if self.hostname == self.master_host:
            # ML_lock.acquire()
            self.ML.append(self.hostname)
            # TODO: tcp to tell everyone the topology
            # for host in self.neighbor_timestamps:
            #     if host == self.master_host:
            #         continue
            #     try:
            #         s = socket.socket()
            #         s.connect((host, MASTER_PORT))
            #         s.send(json.dumps(self.ML).encode())
            #         s.close()
            #         print(f"In join, master also send {self.ML} to {host} successfully")
            #     except:
            #         pass
            # ML_lock.release()
        else:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.master_host, MASTER_PORT))
            s.send(json.dumps(["join", self.hostname]).encode())
            s.close()
    
    def leave(self, host=None):
        if host == None:
            host = self.hostname
        # TODO: send a leave message to the master
        if host == self.master_host:
            # we do not allow master to leave
            print("Cannot force master node to leave!")
            return
        # else send leave message to the master
        elif host == self.hostname:
            # ML_lock.acquire()
            self.online = False
            self.ML = []
            self.init_timestamps()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.master_host, MASTER_PORT))
        s.send(json.dumps(["leave", host]).encode())
        s.close()

    def listen_to_master(self):
        if self.is_master():
            # master no need to listen to itself, just directly manipulate is fine
            # in this case, it is convenient for us to only use one port for both send to and listen from master for each node 
            return
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.hostname, MASTER_PORT))
        s.listen(5)
        while 1:
            if not self.online:
                # print(f"{self.hostname} not online yet")
                time.sleep(0.05)
                continue
            # print(f"{self.hostname} is online and now listen to the master")
            if EXIT_SIGNAL:
                return
            conn, addr = s.accept()
            new_ML = json.loads(conn.recv(4096).decode())
            # print(f"receive new_ML from master: ", new_ML)
            if len(new_ML) >= 2 and new_ML[-1].isdigit():
                if new_ML[-2] == self.hostname:
                    # receive leave message
                    self.ML = []
                    new_neighbors = []
                    self.online = False
                else:
                    self.ML = new_ML[:-2]
            else:
                # receive join message
                if new_ML[-1] in self.ML:
                    print("Node already exist in the ring!")
                    # ML_lock.release()
                    continue
                self.ML = new_ML
                # print("listen to master: ", new_ML)
                new_neighbors = self.get_neighbors()
            # ML_lock.release()
            currTime = time.time()
            # for i, host in enumerate(self.neighbor_timestamps):
            #     if host not in new_neighbors:
            #         self.neighbor_timestamps[host][0] = 0
            #         self.neighbor_timestamps[host][1] = 0
            #     else:
            #         if not self.neighbor_timestamps[host][0]:
            #             self.neighbor_timestamps[host][0] = 1
            #             self.neighbor_timestamps[host][1] = currTime
            print(self.neighbor_timestamps)

    def listen_join_and_leave(self):
        #TODO: run a thread to know who are joining and leaving
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.hostname, MASTER_PORT))
        s.listen(5)
        while 1:
            if EXIT_SIGNAL:
                return
            conn, addr = s.accept()
            # print("accept one time")
            news = json.loads(conn.recv(4096).decode())
            # ML_lock.acquire()
            if news[0] == "leave":
                idx = -1
                for i in range(len(self.ML)):
                    if self.ML[i] == news[1]:
                         idx = i
                         break
                if idx == -1:
                    # This should indicate that multiple nodes detected the leave/fail and reported to the master, we do nothing
                    # raise NotImplementedError("TODO: fix bug in listen_join_and_leave that inactive host is not in the membership list")
                    continue
                send_ML = self.ML[:idx] + self.ML[idx+1:] + [self.ML[idx]] + [str(idx)]
                self.ML = send_ML[:-2]
            elif news[0] == "join":
                self.neighbor_timestamps[news[1]][0] = 1
                self.neighbor_timestamps[news[1]][1] = time.time()
                print(self.neighbor_timestamps[news[1]])
                self.ML.append(news[1])
                send_ML = self.ML
                print("master send join messages: ", send_ML)
            else:
                # ML_lock.release()
                raise NotImplementedError("TODO: fix bug in listen_join_and_leave, the received news has unrecognizable message header")
            # ML_lock.release()
            # TODO: ask everyone to update their membership list
            for host in self.neighbor_timestamps:
                if host == self.master_host:
                    continue
                if self.neighbor_timestamps[host][0]:
                    # print("preparing to send to ", host)
                    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    # print("connecting ", host)
                    s1.connect((host, MASTER_PORT))
                    # print("sending to ", host, " with ML = ", send_ML)
                    s1.send(json.dumps(send_ML).encode())
                    # print("closing connection to ", host)
                    s1.close()
                    # print("successfully send ML: ", send_ML)
                try:
                    # print("preparing to send to ", host)
                    # s1 = socket.socket()
                    # print("connecting ", host)
                    # s1.connect((host, MASTER_PORT))
                    # print("sending to ", host)
                    # s1.send(json.dumps(send_ML).encode())
                    # print("closing connection to ", host)
                    # s1.close()
                    # print("successfully send ML: ", send_ML)
                    pass
                except:
                    pass

    def check_neighbors_alive(self):
        #TODO: check heartbeats
        if not self.is_master():
            return
        while 1:
            # print(self.neighbor_timestamps)
            time.sleep(0.5) # sleep 500ms and check
            currTime = time.time()
            # currNeighbors = self.get_neighbors()
            for host in self.neighbor_timestamps:
                if host == self.master_host or (host == self.hostname):
                    continue
                if host in self.ML and (currTime - self.neighbor_timestamps[host][1]) > 1.5:
                    # some node leave or failed
                    print("host ", host, f" timeout! currTime = {currTime}, node timestamp = {self.neighbor_timestamps[host][1]}")
                    self.neighbor_timestamps[host][0] = 0
                    if host in self.ML:
                        print("leave the host ", host)
                        self.leave(host)
    
    def ping(self):
        # send ping to check neighbors alive every 300 ms
        # print(f"my name is {self.hostname} and self.is_master = {self.is_master()}")
        if self.is_master():
            return
        # s.bind((self.hostname, ACK_PORT))
        while 1:
            # print("in ping-----------------------------")
            time.sleep(0.3)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.master_host, ACK_PORT))
            s.send(self.hostname.encode())
            # print(f"node {self.hostname} send ping to master at time = {time.time()}!")
            s.close()
    
    def receive_ack(self):
        if not self.is_master():
            return
        # print("in receive ack-----------------------------")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.hostname, ACK_PORT))
        s.listen(5)
        while 1:
            conn, addr = s.accept()
            ack_host = conn.recv(4096).decode()
            self.neighbor_timestamps[ack_host][1] = time.time()
            # print("Get ping from joined node ", ack_host, " at timestamp ", self.neighbor_timestamps[ack_host][1])
    
    # def receive_update(self):
    #     # responde to neighbors to show they are alive
    #     s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #     s.bind((self.hostname, ACK_PORT))
    #     while 1:
    #         if not self.online:
    #             continue
    #         query, addr = s.recvfrom(4096)
    #         # print("receive ping from ", query.decode)
    #         if self.online:
    #             # print("since I am online, ack to the ping!")
    #             s.sendto(self.hostname.encode(), (query.decode(), RECEIVE_ACK_PORT))

    def shell(self):
        while 1:
            print("Please input command:")
            command = input()
            if command == "list_mem":
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
            elif command == "exit":
                EXIT_SIGNAL = 1
                self.sucide()
                return
            else:
                print("invalid command, use help to check available commands!")
    
    def run(self):
        t0 = threading.Thread(target=self.listen_join_and_leave, name="listen_join_and_leave") if self.is_master() else threading.Thread(target=self.listen_to_master, name="listen_to_master")
        t1 = threading.Thread(target=self.shell, name="shell")
        t2 = threading.Thread(target=self.ping, name="ping")
        # t3 = threading.Thread(target=self.ack, name="ack")
        t4 = threading.Thread(target=self.receive_ack, name="receive_ack")
        t5 = threading.Thread(target=self.check_neighbors_alive, name="check_neighbors_alive")

        t0.start()
        t1.start()
        t2.start()
        # t3.start()
        t4.start()
        t5.start()
        print('all threads started!')
        t0.join()
        print('t0 join')
        t1.join()
        print('t1 join')
        t2.join()
        print('t2 join')
        # t3.join()
        # print('t3 join')
        t4.join()
        print('t4 join')
        t5.join()
        print('t5 join')

s = Server()
s.run()
print("finish!")