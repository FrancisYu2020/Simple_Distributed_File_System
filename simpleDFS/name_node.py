from collections import defaultdict
import hashlib
import zerorpc
from multiprocessing import Queue, Process
import threading
import socket
import time
import logging
import simpleDFS.failure_detector as failure_detector

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='Namenode.log',
                    filemode='w')
DATA_NODE_PORT = "4242"
NAME_NODE_PORT = 4241
work_queue = Queue(1000)

class File:
    '''
    class File
    contains replicas
    '''
    def __init__(self, filename):
        self.filename = filename
        self.replicas = set()
    
    def __repr__(self):
        return "{ \n\t\"filename\" : \"" + self.filename + "\"\n\t\"replicas\" : " + str(self.replicas) + "\n}"

class FileTable:
    '''
    class FileTable
    contains multiple files
    '''
    def __init__(self):
        self.files = {} # name -> File
    
    def insert_file(self, filename, replicas):
        f = File(filename)
        f.replicas = set(replicas)
        self.files[filename] = f    
    
    def update_replicas(self, filename, replica):
        self.files[filename].replicas.add(replica)
    
    def delete_file(self, filename):
        if filename not in self.files:
            # print("No such file")
            return
        del self.files[filename]

class NameNode:
    '''
    class NameNode
    '''
    def __init__(self, fd): 
        self.ft = FileTable()
        self.fd = fd
        self.ml = ["fa22-cs425-2201.cs.illinois.edu", "fa22-cs425-2202.cs.illinois.edu",
                "fa22-cs425-2203.cs.illinois.edu", "fa22-cs425-2204.cs.illinois.edu",
                "fa22-cs425-2205.cs.illinois.edu"]
        self.work_queue = Queue(1000)
        checker = threading.Thread(target=self.safe_checker)
        checker.start()

    def __hash_sdfs_name(self, sdfs_name):
        '''
        map the sdfs name to replicas
        '''
        m = hashlib.md5()
        m.update(sdfs_name.encode('utf-8'))
        id = int(m.hexdigest(), 16)
        if len(self.ml) < 4:
            return self.ml
        return [self.ml[i % len(self.ml)] for i in range(id, id + 4)]
    
    def __find_rebuild_replicas(self, need_num, cur_replicas):
        '''
        find new replicas which need to store the file for crash nodes
        '''
        ret = []
        for _ in range(need_num):
            for m in self.ml:
                if m not in cur_replicas and m not in ret:
                    ret.append(m)
                    break
        return ret
    
    def __update_ml():
        return

    def initial_mode(self):
        '''
        Use heartbeat message to rebuild name node
        '''
        print("Initial mode...")
        logging.info("Initial Namenode...")
        for node in self.ml:
            c = zerorpc.Client()
            c.connect("tcp://" + node + ":" + DATA_NODE_PORT)
            node_info = c.heartbeat()
            if not node_info:
                continue
            files = node_info.split(" ")
            c.close()
            for file in files:
                if file not in self.ft.files:
                    self.ft.insert_file(file, [node])
                else:
                    self.ft.update_replicas(file, node)
        logging.info("Cur Files: ")
        for file in self.ft.files.keys():
            print(repr(self.ft.files[file]))
            logging.info(repr(self.ft.files[file]))
        print("Finsih initial mode!")
        logging.info("Finsih Initial Namenode.")
    
    def rreplica(self, need_num, cur_replicas, filename):
        '''
        rebuild the file of fail node to new replicas
        '''
        logging.warning("Safe Checker: Start rereplica for " + filename)
        new_replicas = self.__find_rebuild_replicas(need_num, cur_replicas)
        replica = cur_replicas[0]
        c = zerorpc.Client(timeout = 10)
        c.connect("tcp://" + replica + ":" + DATA_NODE_PORT)
        c.rreplica(new_replicas, filename)
        c.close()
        for r in new_replicas:
            self.ft.files[filename].replicas.add(r)
        logging.warning("Safe Checker: New replica contains " + str(new_replicas))
        return

    def safe_checker(self):
        '''
        check all files to make sure all files have enough replicas
        '''
        print("Safe checker is running...")
        logging.info("Safe Checker start")
        while True:
            try:
                for file in self.ft.files.keys():
                    replica_num = len(self.ft.files[file].replicas)
                    if replica_num < 4:
                        logging.warning("Safe Checker: File " + file + " lack " + str(4 - replica_num) + " replica(s).")
                        self.rreplica(4 - replica_num, list(self.ft.files[file].replicas), file)
                time.sleep(1)
            except Exception as e:
                logging.error("Safe Checker: Rereplica Failed. Start try again...")
                continue
        return 


    def put_file(self, sdfs_name):
        if sdfs_name not in self.ft.files:
            replicas = self.__hash_sdfs_name(sdfs_name)
        else:
            replicas = list(self.ft.files[sdfs_name].replicas)
            self.ft.insert_file(sdfs_name, replicas)
        return replicas

    def get_file(self, sdfs_name):
        if sdfs_name in self.ft.files:
            return " ".join(list(self.ft.files[sdfs_name].replicas))
        else:
            print("No such file!")
            return

    def delete_file(self, sdfs_name):
        # logging.info("Namenode is deleting file: " + sdfs_name)
        print("Namenode is deleting file: ", sdfs_name)
        if sdfs_name not in self.ft.files:
            print("No such file")
            # logging.info("No such file")
            return
        replicas = self.ft.files[sdfs_name].replicas
        print(replicas)
        try:
            for r in replicas:
                c = zerorpc.Client(timeout=10)
                c.connect("tcp://" + r + ":" + DATA_NODE_PORT)
                c.delete_file(sdfs_name)
                c.close()
        except Exception as e:
            logging.error(str(e))
            print(e)
            return False
        self.ft.delete_file(sdfs_name)
        return True

    def ls(self, sdfs_name):
        if sdfs_name not in self.ft.files:
            return
        return repr(self.ft.files[sdfs_name])

    def store(self, client):
        c = zerorpc.Client(timeout=5)
        c.connect("tcp://" + client + ":" + DATA_NODE_PORT)
        return c.heartbeat()
    
    def producer(self):
        print("Producer is running")
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        localaddr = (socket.gethostname(), NAME_NODE_PORT)
        udp_socket.bind(localaddr)
        while True:
            command, client_addr = udp_socket.recvfrom(4096)
            command = command.decode("utf-8")
            print("receive command: " + command + ", from " + str(client_addr[0]))
            work_queue.put((command, client_addr))
        s.close()
        udp_socket.close()


def run(fd):
    name_node = NameNode(fd)
    name_node.initial_mode()
    # safe_checker = 
    # name_node.safe_checker()
    print("NameNode is running")
    logging.info("NameNode Start")
    pro = Process(target=name_node.producer)
    pro.start()
    print("Consumer is running")
    logging.info("Consumer Start")
    while True:
        command, client_addr = work_queue.get()
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if command:
            try:
                args = command.split(" ")
                if args[0] == "put":
                    print("Receive put request")
                    logging.info("Receive put request: " + args[1])
                    data = " ".join(name_node.put_file(args[1])).encode("utf-8")
                    s.sendto(data, client_addr)
                elif args[0] == "get":
                    print("Receive get request")
                    logging.info("Receive get request: " + args[1])
                    data = name_node.get_file(args[1])
                    if not data:
                        data = ""
                    s.sendto(data.encode("utf-8"), client_addr)
                elif args[0] == "delete":
                    print("Receive delete request")
                    logging.info("Receive delete request: " + args[1])
                    if name_node.delete_file(args[1]):
                        data = "ack"
                    else:
                        data = "nack"
                    s.sendto(data.encode("utf-8"), client_addr)
                elif args[0] == "ls":
                    print("Receive ls request: " + args[1])
                    logging.info("Receive ls request: " + args[1])
                    data = name_node.ls(args[1])
                    if not data:
                        data = "Oops! No such file."
                    s.sendto(data.encode("utf-8"), client_addr)
                elif args[0] == "store":
                    logging.info("Receive ls request")
                    data = name_node.store(client_addr[0]).encode("utf-8")
                    s.sendto(data, client_addr)
            except Exception as e:
                print(e)
                logging.error("Operation failed" + str(e))
                data = "Operation failed, please try again.".encode("utf-8")
                s.sendto(data, client_addr)
        else:
            s.close()
            break

run()