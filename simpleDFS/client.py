import zerorpc
import socket
import os

DATA_NODE_PORT = "6242"
NAME_NODE_PORT = 6241

class Client:
    def __init__(self):
        pass
    
    def get_namenode_host(self):
        return "fa22-cs425-2210.cs.illinois.edu"
    
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
                elif args[0] == "get":
                    self.get(args[1], args[2])
                else:
                    print("Invalid Input! If you need any help, please type 'help' for instructions.")
                    continue
            elif len(args) == 2:
                if args[0] == "delete":
                    self.delete(args[1])
                elif args[0] == "ls":
                    self.ls(args[1])
                else:
                    print("Invalid Input! If you need any help, please type 'help' for instructions.")
                    continue
            elif len(args) == 4:
                if args[0] == "get-versions":
                    sdfs_filename = args[1]
                    numversions = args[2]
                    localfilename = args[3]
                    self.get_versions(sdfs_filename, int(numversions), localfilename)
                else:
                    print("Invalid Input! If you need any help, please type 'help' for instructions.")
                    continue
            elif len(args) == 1:
                if args[0] == "store":
                    self.store()
                elif args[0] == "exit":
                    return
                elif args[0] == "help":
                    self.printCommands()
                else:
                    print("Invalid Input! If you need any help, please type 'help' for instructions.")
                    continue
            
    def put(self, local_filename, sdfs_filename):
        if not os.path.exists(local_filename):
            print("No such local file, please try again.")
            return
        
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        dst_addr = (self.get_namenode_host(), NAME_NODE_PORT)
        data = "put " + sdfs_filename
        s.sendto(data.encode("utf-8"), dst_addr)
        replicas, _ = s.recvfrom(4096)
        replicas = replicas.decode("utf-8").split(" ")
        
        content = open(local_filename, "rb").read()
        c = zerorpc.Client(timeout=None)
        c.connect("tcp://" + replicas[0] + ":" + DATA_NODE_PORT)
        c.put_file(sdfs_filename, content, replicas[1:])
        c.close()
        s.settimeout(30)
        finish, _ = s.recvfrom(4096)
        s.close()
        print(finish.decode("utf-8"))
        if finish.decode("utf-8") == "finish":
            print("Put Success.")
        else:
            print("Fail")
    
    def get(self, sdfs_filename, local_filename):
        # get address
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        dst_addr = (self.get_namenode_host(), NAME_NODE_PORT)
        data = "get " + sdfs_filename
        s.sendto(data.encode("utf-8"), dst_addr)
        replicas, _ = s.recvfrom(4096)
        replicas = replicas.decode("utf-8")
        s.close()
        if not replicas:
            print("Oops, no such file.")
            return
        replicas = replicas.split(" ")

        quorum = version = 0
        write_content = None
        for replica in replicas:
            # write to local
            try:
                c = zerorpc.Client(timeout=None)
                c.connect("tcp://" + replica + ":" + DATA_NODE_PORT)
                content, v = c.get_file(sdfs_filename)
                c.close()
                if v > version:
                    version, write_content = v, content
                quorum += 1
                if quorum == 2:
                    f = open(local_filename, 'wb')
                    f.write(write_content)
                    print("Get Success.")
                    return
            except:
                continue
        if quorum == 1:
            f = open(local_filename, 'wb')
            f.write(write_content)
            print("Get Success.")
        print("Fail, please try again.")
    
    def delete(self, sdfs_filename):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        dst_addr = (self.get_namenode_host(), NAME_NODE_PORT)
        data = "delete " + sdfs_filename
        s.sendto(data.encode("utf-8"), dst_addr)
        ack, _ = s.recvfrom(4096)
        s.close()
        if ack.decode("utf-8") == "nack":
            print("Fail")
        else:
            print("Success")
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
    
    def get_versions(self, sdfs_filename, versions, local_filename):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        dst_addr = (self.get_namenode_host(), NAME_NODE_PORT)
        data = "get " + sdfs_filename
        s.sendto(data.encode("utf-8"), dst_addr)
        replicas, _ = s.recvfrom(4096)
        replicas = replicas.decode("utf-8")
        s.close()
        if not replicas:
            print("Oops, no such file.")
            return

        replicas = replicas.split(" ")
        for replica in replicas:
            # write to local
            try:
                for v in range(versions):
                    c = zerorpc.Client(timeout=None)
                    c.connect("tcp://" + replica + ":" + DATA_NODE_PORT)
                    content, version = c.get_file_version(sdfs_filename, v)
                    c.close()
                    if version > 0:
                        f = open(local_filename + ",v" + str(version), 'wb')
                        f.write(content)
                    elif version == -1:
                        print("All versions are received, no other previous versions.")
                        return
                    elif version == -2:
                        print("Failed, please try again.")
                print("Success.")
                return
            except:
                continue
        print("Fail, please try again.")

c = Client()
c.run()
