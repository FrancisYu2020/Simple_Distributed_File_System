import zerorpc
import os

class DataNode:
    def __init__(self):
        pass

    def put_file(self, sdfs_filename, content):
        filepath = os.path.join(os.getcwd(), sdfs_filename)
        f = open(filepath, 'w')
        f.write(content)
        f.close()

    def get_file(self, sdfs_filename):
        filepath = os.path.join(os.getcwd(), sdfs_filename)
        if not os.path.isfile(filepath):
            print("No file")
            return
        return open(filepath).read()

    def delete_file(self, sdfs_filename):
        filepath = os.path.join(os.getcwd(), sdfs_filename)
        os.remove(filepath)

def run_data_node():
    s = zerorpc.Server(DataNode())
    s.bind("tcp://0.0.0.0:4242")
    print("DataNode Server is running!")
    s.run()

run_data_node()