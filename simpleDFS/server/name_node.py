from collections import defaultdict
import hashlib
import zerorpc
from enum import Enum


DATA_NODE_PORT = "4242"
NAME_NODE_PORT = "4241"

class State(Enum):
    IDLE = 0
    WORK = 1
    FAIL = 2

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
    
    def update_replicas(self, filename, replicas):
        self.files[filename].replicas = set(replicas)
    
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
                "fa22-cs425-2205.cs.illinois.edu", "fa22-cs425-2206.cs.illinois.edu", 
                "fa22-cs425-2207.cs.illinois.edu", "fa22-cs425-2208.cs.illinois.edu",
                "fa22-cs425-2209.cs.illinois.edu", "fa22-cs425-2210.cs.illinois.edu"]

    def __hash_sdfs_name(self, sdfs_name):
        m = hashlib.md5()
        m.update(sdfs_name.encode('utf-8'))
        id = int(m.hexdigest(), 16)
        if len(self.ml) < 4:
            return self.ml
        return [self.ml[i % len(self.ml)] for i in range(id, id + 4)]
        # return ["fa22-cs425-2205.cs.illinois.edu", "fa22-cs425-2206.cs.illinois.edu", "fa22-cs425-2207.cs.illinois.edu", "fa22-cs425-2208.cs.illinois.edu"]

    def safe_mode(self):
        for node in self.ml:
            c = zerorpc.Client()
            c.connect("tcp://" + node + ":" + DATA_NODE_PORT)
            node_info = c.safe_mode()

    def put_file(self, sdfs_name):
        print("Receive put request")
        if sdfs_name not in self.ft.files:
            replicas = self.__hash_sdfs_name(sdfs_name)
            self.nt.insert_file(sdfs_name, replicas)
            self.ft.insert_file(sdfs_name, replicas)
        return self.ft.files[sdfs_name].replicas

    def get_file(self, sdfs_name):
        print("Receive get request")
        if sdfs_name in self.ft.files:
            return self.ft.files[sdfs_name].replicas[0]
        else:
            print("No such file!")
            return

    def delete_file(self, sdfs_name):
        print("Receive delete request")
        replicas = self.ft.files[sdfs_name].replicas
        for replica in replicas:
            c = zerorpc.Client()
            c.connect("tcp://" + replica + ":" + DATA_NODE_PORT)
            c.delete_file(sdfs_name)
        self.ft.delete_file(sdfs_name)
        self.nt.delete_file(sdfs_name)
        return 

    def ls(self, sdfs_name):
        return repr(self.ft.files[sdfs_name])

    def store(self, data_node):
        return repr(self.nt[data_node])

    def rreplcia(self, fail_node, new_replica):
        files = self.nt.nodes[fail_node]
        for file in files:
            self.ft.files[file].replicas.remove(fail_node)
            for replica in self.ft.files[file].replicas:
                if replica != fail_node:
                    c = zerorpc.Client()
                    c.connect("tcp://" + replica + ":" + DATA_NODE_PORT)
                    c.build_replica(file, new_replica)
                    c.close()
                    self.ft.files[file].replicas.add(new_replica)
                    break
        del self.nt.nodes[fail_node]
            

    
def run_name_node():
    s = zerorpc.Server(NameNode())
    s.bind("tcp://0.0.0.0:" + NAME_NODE_PORT)
    print("NameNode Server is running!")
    s.run()

run_name_node()