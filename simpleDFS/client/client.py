import zerorpc
import socket

DATA_NODE_PORT = "4242"
NAME_NODE_PORT = 4241

class Client:
    def __init__(self):
        pass
    
    def get_namenode_host(self):
        return "fa22-cs425-2205.cs.illinois.edu"
    
    def printCommands(self):
        commands = '''
        ***********************  Commands  *************************
        1. put <localfilename> <sdfsfilename>
        2. get <sdfsfilename> <localfilename>
        3. delete <sdfsfilename>
        4. ls <sdfsfilename>
        5. store
        6. get-versions <sdfsfilename> <numversions> <localfilename>
        7. exit
        8. help
        ************************************************************
        '''
        print(commands)
        return

    def run(self):
        self.printCommands()
        while True:
            command = input("Please input command:\n")
            args = command.split(" ")
            if len(args) == 3:
                if args[0] == "put":
                    self.put(args[1], args[2])
                    print("Put Success")
                elif args[0] == "get":
                    self.get(args[1], args[2])
                    print("Get Success")
                else:
                    print("Invalid Input! If you need any help, please type help for instructions.")
                    continue
            elif len(args) == 2:
                if args[0] == "delete":
                    self.delete(args[1])
                    print("Delete Success")
                elif args[0] == "ls":
                    self.ls(args[1])
                else:
                    print("Invalid Input! If you need any help, please type help for instructions.")
                    continue
            elif len(args) == 4:
                pass
            elif len(args) == 1:
                if args[0] == "store":
                    self.store()
                elif args[0] == "exit":
                    return
                elif args[0] == "help":
                    self.printCommands()
                else:
                    print("Invalid Input! If you need any help, please type help for instructions.")
                    continue
            
    def put(self, local_filename, sdfs_filename):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        dst_addr = (self.get_namenode_host(), NAME_NODE_PORT)
        data = "put " + sdfs_filename
        s.sendto(data.encode("utf-8"), dst_addr)
        replicas, _ = s.recvfrom(4096)
        replicas = replicas.decode("utf-8").split(" ")
        print(replicas)
        s.close()

        content = open(local_filename, "rb").read()
        c = zerorpc.Client()
        c.connect("tcp://" + replicas[0] + ":" + DATA_NODE_PORT)
        c.put_file(sdfs_filename, content, replicas[1:])
        c.close()
    
    def get(self, sdfs_filename, local_filename):
        # get address
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        dst_addr = (self.get_namenode_host(), NAME_NODE_PORT)
        data = "get " + sdfs_filename
        s.sendto(data.encode("utf-8"), dst_addr)
        replica, _ = s.recvfrom(4096)
        replica = replica.decode("utf-8")
        s.close()

        # write to local
        c = zerorpc.Client()
        c.connect("tcp://" + replica + ":" + DATA_NODE_PORT)
        content = c.get_file(sdfs_filename)
        c.close()
        f = open(local_filename, 'wb')
        f.write(content)
    
    def delete(self, sdfs_filename):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        dst_addr = (self.get_namenode_host(), NAME_NODE_PORT)
        data = "delete " + sdfs_filename
        s.sendto(data.encode("utf-8"), dst_addr)
        ack, _ = s.recvfrom(4096)
        s.close()
        if ack.decode("utf-8") == "nack":
            print("Fail")
        return
    
    def ls(self, sdfs_filename):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        dst_addr = (self.get_namenode_host(), NAME_NODE_PORT)
        data = "ls " + sdfs_filename
        s.sendto(data.encode("utf-8"), dst_addr)
        file_info, _ = s.recvfrom(4096)
        print(file_info.decode("utf-8"))

    def store(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        dst_addr = (self.get_namenode_host(), NAME_NODE_PORT)
        data = "store"
        s.sendto(data.encode("utf-8"), dst_addr)
        file_info, _ = s.recvfrom(4096)
        print(file_info.decode("utf-8"))
    

c = Client()
c.run()
