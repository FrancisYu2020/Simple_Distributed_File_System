import zerorpc
import os
from collections import defaultdict
import logging
import threading
import grep_server
import socket

DATA_NODE_PORT = "6242"
ACK_PORTS = [6000 + i for i in range(11)]

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='Datanode.log',
                    filemode='w')

class DataNode:
    def __init__(self):
        self.file_info = defaultdict(int)   # sdfs_filename -> version
        self.host_name = socket.gethostname()
        self.host_id = int(self.host_name[13:15])
    
    def get_namenode_host(self):
        return "fa22-cs425-2210.cs.illinois.edu"

    def put_file(self, sdfs_filename, content, replicas):
        forward_file_t = threading.Thread(target=self.forward_file, args=[sdfs_filename, content, replicas])
        forward_file_t.start()
        return

    def forward(self, sdfs_filename, content, replicas):
        print("Try to forward to:")
        logging.info("Try to forward")
        print(replicas)
        if replicas:
            try:
                c = zerorpc.Client(timeout=None)
                c.connect("tcp://" + replicas[0] + ":" + DATA_NODE_PORT)
                c.put_file(sdfs_filename, content, replicas[1:])
                c.close()
            except Exception as e:
                logging.error("Forward error " + str(e))
        print("Forward end")
        logging.info("Forward end")


    def forward_file(self, sdfs_filename, content, replicas):
        if not os.path.exists(os.getcwd() + "/store"):
            os.makedirs(os.getcwd() + "/store")
        forward_t = threading.Thread(target=self.forward, args=[sdfs_filename, content, replicas])
        forward_t.start()
        print("Try to put file: " + sdfs_filename)
        logging.info("Try to put file: " + sdfs_filename)
        filename = sdfs_filename + ",v" + str(self.file_info[sdfs_filename] + 1)
        filepath = os.path.join(os.getcwd() + "/store", filename)
        f = open(filepath, "wb")
        f.write(content)
        f.close()
        logging.info("Put file end: " + sdfs_filename)
        ack = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        dst_addr = (self.get_namenode_host(), ACK_PORTS[self.host_id])
        data = "ack"
        ack.sendto(data.encode("utf-8"), dst_addr)
        ack.close()

        self.file_info[sdfs_filename] += 1
        logging.info("Put file " + filename + ", current version is " + str(self.file_info[sdfs_filename]))

    def get_file(self, sdfs_filename):
        print("Try to get file: " + sdfs_filename)
        filepath = os.path.join(os.getcwd() + "/store", sdfs_filename + ",v" + str(self.file_info[sdfs_filename]))
        if not os.path.isfile(filepath):
            print("No file")
            return
        logging.info("Get file " + sdfs_filename + ", current version is " + str(self.file_info[sdfs_filename]))
        return open(filepath, "rb").read(), self.file_info[sdfs_filename]
    
    def get_file_version(self, sdfs_filename, v):
        print("Try to get file: " + sdfs_filename)
        if v >= self.file_info[sdfs_filename]:
            return "", -1
        filepath = os.path.join(os.getcwd() + "/store", sdfs_filename + ",v" + str(self.file_info[sdfs_filename] - v))
        if not os.path.isfile(filepath):
            print("No file: " + filepath)
            return "", -2
        logging.info("Get file " + sdfs_filename + "for specific version: " + str(self.file_info[sdfs_filename] - v))
        return open(filepath, "rb").read(), self.file_info[sdfs_filename] - v

    def delete_file(self, sdfs_filename):
        print("Try to delete file: " + sdfs_filename)
        if sdfs_filename not in self.file_info:
            print("No such file")
            return
        for v in range(1, self.file_info[sdfs_filename] + 1):
            filepath = os.path.join(os.getcwd() + "/store", sdfs_filename + ",v" + str(v))
            os.remove(filepath)
        del self.file_info[sdfs_filename]
        logging.info("Delete file " + sdfs_filename)
    
    def rreplica(self, new_replicas, sdfs_filename):
        logging.info("Rereplica file " + sdfs_filename + " to " + str(new_replicas))
        try:
            for replica in new_replicas:
                for v in range(1, self.file_info[sdfs_filename] + 1):
                    filepath = os.path.join(os.getcwd() + "/store", sdfs_filename + ",v" + str(v))
                    content = open(filepath, "rb").read()
                    c = zerorpc.Client(timeout=None)
                    c.connect("tcp://" + replica + ":" + DATA_NODE_PORT)
                    c.rebuild(sdfs_filename, content, v)
                    c.close()
        except Exception as e:
            logging.ERROR("Replica failed, error is : " + str(e))

    def rebuild(self, sdfs_filename, content, v):
        logging.info("Rebuild file " + sdfs_filename)
        filename = sdfs_filename + ",v" + str(v)
        filepath = os.path.join(os.getcwd() + "/store", filename)
        f = open(filepath, "wb")
        f.write(content)
        f.close()
        self.file_info[sdfs_filename] = v
    
    def heartbeat(self):
        print("Heartbeat")
        if not os.path.exists(os.getcwd() + "/store"):
            os.makedirs(os.getcwd() + "/store")
            return ""
        files = os.listdir(os.getcwd() + "/store")
        ret = [file.split(",")[0] for file in files]
        logging.info("Heartbeat: " + " ".join(list(set(ret))))
        return " ".join(list(set(ret)))

def run_data_node():
    grep_t = threading.Thread(target = grep_server.server_program)
    grep_t.start()
    s = zerorpc.Server(DataNode(), heartbeat=60)
    s.bind("tcp://0.0.0.0:" + DATA_NODE_PORT)
    print("DataNode Server is running!")
    logging.info("DataNode Start")
    s.run()
    grep_server.join()
    

# run_data_node()