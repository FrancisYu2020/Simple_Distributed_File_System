import zerorpc
import os

DATA_NODE_PORT = "4242"
INTER_DATA_NODE_PORT = "4243"

class DataNode:
    def __init__(self):
        pass

    def put_file(self, sdfs_filename, content, replicas):
        print("Try to put file: " + sdfs_filename)
        filepath = os.path.join(os.getcwd(), sdfs_filename)
        f = open(filepath, "wb")
        f.write(content)
        f.close()
        if replicas:
            c = zerorpc.Client()
            c.connect("tcp://" + replicas[0] + ":" + INTER_DATA_NODE_PORT)
            c.put_file(sdfs_filename, content, replicas[1:])
            c.close()

    def get_file(self, sdfs_filename):
        print("Try to get file: " + sdfs_filename)
        filepath = os.path.join(os.getcwd(), sdfs_filename)
        if not os.path.isfile(filepath):
            print("No file")
            return
        return open(filepath, "rb").read()

    def delete_file(self, sdfs_filename):
        print("Try to delete file: " + sdfs_filename)
        filepath = os.path.join(os.getcwd(), sdfs_filename)
        os.remove(filepath)

def run_data_node():
    s = zerorpc.Server(DataNode())
    s.bind("tcp://0.0.0.0:" + DATA_NODE_PORT)
    print("DataNode Server is running!")
    s.run()

run_data_node()