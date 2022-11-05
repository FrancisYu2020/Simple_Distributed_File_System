import name_node
import data_node
import fd
from multiprocessing import Process


class Server:
    def __init__(self):
        self.fd = fd.Server("fa22-cs425-2205.cs.illinois.edu")
        self.nn = name_node.NameNode(self.fd)
        self.dn = data_node.DataNode()

    def run(self):
        p = Process(target=self.fd.run)
        p1 = Process(target=self.nn.run)
        p2 = Process(target=self.dn.run_data_node)
        p.start()
        p1.start()
        p2.start()
    

if __name__ == '__main__':
    s = Server()
    s.run()
