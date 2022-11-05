import name_node
import data_node
import fd
from multiprocessing import Process
import threading


class Server:
    def __init__(self):
        self.fd = fd.Server("fa22-cs425-2205.cs.illinois.edu")
        self.nn = name_node.NameNode(self.fd)
        self.dn = data_node.DataNode()

    def run(self):
        t0 = threading.Thread(target=self.fd.run)
        t1 = threading.Thread(target=self.nn.run_name_node)
        t2 = threading.Thread(target=self.dn.run_data_node)
        t0.start()
        t1.start()
        t2.start()
    

if __name__ == '__main__':
    s = Server()
    s.run()
