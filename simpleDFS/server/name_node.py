from collections import defaultdict
import hashlib
import zerorpc
from multiprocessing import Queue, Process
import socket


DATA_NODE_PORT = "4242"
NAME_NODE_PORT = 4241

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
            print("No such file")
            return
        del self.files[filename]

class NodeTable:
    '''
    class NodeTable
    contains nodes and the files in the node
    '''
    def __init__(self):
        self.nodes = defaultdict(set)
    
    def insert_file(self, filename, replicas):
        for r in replicas:
            self.nodes[r].add(filename)
    
    def delete_file(self, filename):
        for node in self.nodes.keys():
            if filename in self.nodes[node]:
                self.nodes[node].remove(filename)

class NameNode:
    '''
    class NameNode
    '''
    def __init__(self): 
        self.ft = FileTable()
        self.nt = NodeTable()
        self.ml = ["fa22-cs425-2201.cs.illinois.edu", "fa22-cs425-2202.cs.illinois.edu",
                "fa22-cs425-2203.cs.illinois.edu", "fa22-cs425-2204.cs.illinois.edu",
                "fa22-cs425-2205.cs.illinois.edu", "fa22-cs425-2206.cs.illinois.edu"]
        self.work_queue = Queue(1000)

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
        return ret

    def initial_mode(self):
        '''
        Use heartbeat message to rebuild name node
        '''
        for node in self.ml:
            c = zerorpc.Client()
            c.connect("tcp://" + node + ":" + DATA_NODE_PORT)
            node_info = c.heartbeat()
            files = node_info.split(" ")
            c.close()
            for file in files:
                self.nt.insert_file(file, [node])
                if file not in self.ft.files:
                    self.ft.insert_file(file, [node])
                else:
                    self.ft.update_replicas(file, node)
        for file in self.ft.files.keys():
            print(repr(self.ft.files[file]))
    
    def rreplica(self, need_num, cur_replicas, filename):
        '''
        rebuild the file of fail node to new replicas
        '''
        new_replicas = self.__find_rebuild_replicas(need_num, cur_replicas)
        replica = cur_replicas[0]
        c = zerorpc.Client()
        c.connect("tcp://" + replica + ":" + DATA_NODE_PORT)
        c.rreplica(new_replicas, filename)
        c.close()

    def safe_mode(self):
        '''
        check all files to make sure all files have enough replicas
        '''
        print("Start safe mode")
        for file in self.ft.files.keys():
            replica_num = len(self.ft.files[file].replicas)
            if replica_num < 4:
                self.rreplica(4 - replica_num, list(self.ft.files[file].replicas), file)
        print("Safe mode finished")
        return 


    def put_file(self, sdfs_name):
        if sdfs_name not in self.ft.files:
            replicas = self.__hash_sdfs_name(sdfs_name)
            self.nt.insert_file(sdfs_name, replicas)
            self.ft.insert_file(sdfs_name, replicas)
        return list(self.ft.files[sdfs_name].replicas)

    def get_file(self, sdfs_name):
        if sdfs_name in self.ft.files:
            return list(self.ft.files[sdfs_name].replicas)[0]
        else:
            print("No such file!")
            return

    def delete_file(self, sdfs_name):
        replicas = self.ft.files[sdfs_name].replicas
        print("delete " + sdfs_name + " from replicas:" + str(replicas))
        for replica in replicas:
            print("Delete", replica)
            c = zerorpc.Client()
            c.connect("tcp://" + replica + ":" + DATA_NODE_PORT)
            print("Connected to " + "tcp://" + replica + ":" + DATA_NODE_PORT)
            c.delete(sdfs_name)
            c.close()
        self.ft.delete_file(sdfs_name)
        self.nt.delete_file(sdfs_name)
        return replicas

    def ls(self, sdfs_name):
        return repr(self.ft.files[sdfs_name])

    def store(self, client):
        print(client)
        c = zerorpc.Client()
        c.connect("tcp://" + client + ":" + DATA_NODE_PORT)
        data = c.heartbeat()
        c.close()
        return data
    
def producer(name_node):
    print("Producer is running")
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    localaddr = (socket.gethostname(), NAME_NODE_PORT)
    print(localaddr)
    udp_socket.bind(localaddr)
    while True:
        command, client_addr = udp_socket.recvfrom(4096)
        command = command.decode("utf-8")
        print("receive command: " + command + ", from " + str(client_addr[0]))
        name_node.work_queue.put((command, client_addr))
    s.close()
    udp_socket.close()


def consumer(name_node):
    print("Consumer is running")
    while True:
        command, client_addr = name_node.work_queue.get()
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if command:
            args = command.split(" ")
            if args[0] == "put":
                print("Receive put request")
                data = " ".join(name_node.put_file(args[1])).encode("utf-8")
                s.sendto(data, client_addr)
            elif args[0] == "get":
                print("Receive get request")
                data = name_node.get_file(args[1]).encode("utf-8")
                s.sendto(data, client_addr)
            elif args[0] == "delete":
                print("Receive delete request")
                name_node.delete_file(args[1])
                s.sendto("ack", client_addr)
            elif args[0] == "ls":
                data = name_node.ls(args[1]).encode("utf-8")
                s.sendto(data, client_addr)
            elif args[0] == "store":
                data = name_node.store(client_addr[0]).encode("utf-8")
                s.sendto(data, client_addr)
        else:
            s.close()
            break
            

def run():
    name_node = NameNode()
    print("Initial namenode")
    name_node.initial_mode()
    name_node.safe_mode()
    # name_node.delete_file("test1")
    print("NameNode is running")
    pro = Process(target=producer, args=([name_node]))
    con = Process(target=consumer, args=([name_node]))
    pro.start()
    con.start()

run()