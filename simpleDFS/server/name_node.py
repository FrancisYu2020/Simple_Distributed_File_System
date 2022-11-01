from collections import defaultdict
import hashlib
import zerorpc


DATA_NODE_PORT = "4242"
NAME_NODE_PORT = "4241"

'''
class File
contains version information
contains replicas
'''
class File:
    def __init__(self, filename):
        self.filename = filename
        self.versions = [0]
        self.replicas = []
    
    def __repr__(self):
        return "{ \n\t\"filename\" : \"" + self.filename + "\"\n\t\"replicas\" : " + str(self.replicas) + "\n\t\"versions\" : " + str(self.versions) + "\n}"

'''
class FileTable
contains multiple files
'''
class FileTable:
    def __init__(self):
        self.files = {} # name -> File
    
    def insert_file(self, filename, replicas):
        f = File(filename)
        f.replicas = replicas
        self.files[filename] = f    
    
    def update_version(self, filename):
        v = self.files[filename].versions[-1]
        self.files[filename].versions.append(v)
    
    def update_replicas(self, filename, replicas):
        self.files[filename].replicas = replicas
    
    def delete_file(self, filename):
        if filename not in self.files:
            print("No such file")
            return
        del self.files[filename]

'''
class NodeTable
contains nodes and the files in the node
'''
class NodeTable:
    def __init__(self):
        self.nodes = defaultdict(set)
    
    def insert_file(self, filename, replicas):
        for r in replicas:
            self.nodes[r].add(filename)
    
    def delete_file(self, filename):
        for node in self.nodes.keys():
            if filename in self.nodes[node]:
                self.nodes[node].remove(filename)

'''
class NameNode
'''
class NameNode:
    def __init__(self): 
        self.ft = FileTable()
        self.nt = NodeTable()
        self.ml = ["fa22-cs425-2205.cs.illinois.edu", "fa22-cs425-2206.cs.illinois.edu", "fa22-cs425-2207.cs.illinois.edu", "fa22-cs425-2208.cs.illinois.edu"]

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


    def put_file(self, sdfs_name):
        print("Receive put request")
        if sdfs_name in self.ft.files:
            self.ft.update_version(sdfs_name)
            return self.ft.files[sdfs_name].replicas
        else:
            replicas = self.__hash_sdfs_name(sdfs_name)
            self.nt.insert_file(sdfs_name, replicas)
            self.ft.insert_file(sdfs_name, replicas)
            return replicas

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
    
def run_name_node():
    s = zerorpc.Server(NameNode())
    s.bind("tcp://0.0.0.0:" + NAME_NODE_PORT)
    print("NameNode Server is running!")
    s.run()

run_name_node()