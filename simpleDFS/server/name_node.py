from collections import defaultdict
import hashlib
import zerorpc
        

class NameNode:
    def __init__(self): 
        self.file_table = {}
        self.node_table = defaultdict(set)

    def __hash_sdfs_name(self, sdfs_name):
        m = hashlib.md5()
        m.update(sdfs_name.encode('utf-8'))
        id = int(m.hexdigest(), 16)
        num = len(self.node_table)
        if num < 4:
            return [i for i in range(num)]
        return [id % num, (id + 1) % num, (id + 2) % num, (id + 3) % num]

    def put_file(self, sdfs_name):
        target_nodes = self.__hash_sdfs_name(sdfs_name)
        # update node table
        for node in target_nodes:
            self.node_table[node].add(sdfs_name)
        # update file table
        if sdfs_name not in self.file_table:
            self.file_table[sdfs_name] = {'version' : 0, 'replicas' : set(target_nodes)}
        else:
            self.file_table[sdfs_name]['version'] += 1
        return ["127.0.0.1"]

    def get_file(self, sdfs_name):
        if sdfs_name in self.file_table:
            return "127.0.0.1"
        else:
            print("No such file!")
            return

    def delete_file(self, sdfs_name):
        addresses = ["127.0.0.1"]
        for address in addresses:
            c = zerorpc.Client()
            c.connect("tcp://" + address + ":4242")
            c.delete_file(sdfs_name)

    def ls(self, sdfs_name):
        return str(self.file_table[sdfs_name])

    def store(self, data_node_id):
        return str(self.node_table[data_node_id])
    
def run_name_node():
    s = zerorpc.Server(NameNode())
    s.bind("tcp://0.0.0.0:4241")
    print("NameNode Server is running!")
    s.run()

run_name_node()