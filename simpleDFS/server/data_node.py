import zerorpc
import os
from collections import defaultdict

DATA_NODE_PORT = "4242"

class DataNode:
    def __init__(self):
        self.file_info = defaultdict(int)   # sdfs_filename -> version

    def put_file(self, sdfs_filename, content, replicas):
        print("Try to put file: " + sdfs_filename)
        filename = sdfs_filename + ",v" + str(self.file_info[sdfs_filename] + 1)
        filepath = os.path.join(os.getcwd(), filename)
        f = open(filepath, "wb")
        f.write(content)
        f.close()
        if replicas:
            c = zerorpc.Client()
            c.connect("tcp://" + replicas[0] + ":" + DATA_NODE_PORT)
            c.put_file(sdfs_filename, content, replicas[1:])
            c.close()
        self.file_info[sdfs_filename] += 1

    def get_file(self, sdfs_filename):
        print("Try to get file: " + sdfs_filename)
        filepath = os.path.join(os.getcwd(), sdfs_filename + ",v" + str(self.file_info[sdfs_filename]))
        if not os.path.isfile(filepath):
            print("No file")
            return
        return open(filepath, "rb").read()

    def delete_file(self, sdfs_filename):
        print("Try to delete file: " + sdfs_filename)
        for v in range(1, self.file_info[sdfs_filename] + 1):
            filepath = os.path.join(os.getcwd(), sdfs_filename + ",v" + str(v))
            os.remove(filepath)
    
    def build_replica(self, sdfs_filename, replica):
        for v in range(1, self.file_info[sdfs_filename] + 1):
            filepath = os.path.join(os.getcwd(), sdfs_filename + ",v" + str(v))
            content = open(filepath, "rb").read()
            c = zerorpc.Client()
            c.connect("tcp://" + replica + ":" + DATA_NODE_PORT)
            c.rreplica(sdfs_filename, content, v)
            c.close()
    
    def rreplica(self, sdfs_filename, content, v):
        filename = sdfs_filename + ",v" + str(v)
        filepath = os.path.join(os.getcwd(), filename)
        f = open(filepath, "wb")
        f.write(content)
        f.close()
        self.file_info[sdfs_filename] = v

def run_data_node():
    s = zerorpc.Server(DataNode())
    s.bind("tcp://0.0.0.0:" + DATA_NODE_PORT)
    print("DataNode Server is running!")
    s.run()

run_data_node()