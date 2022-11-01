import zerorpc

DATA_NODE_PORT = "4242"
NAME_NODE_PORT = "4241"

class Client:
    def __init__(self):
        pass
    
    def get_master_host(self):
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
                    pass
                elif args[0] == "exit":
                    return
                elif args[0] == "help":
                    self.printCommands()
                else:
                    print("Invalid Input! If you need any help, please type help for instructions.")
                    continue
            
    def put(self, local_filename, sdfs_filename):
        
        c = zerorpc.Client()
        c.connect("tcp://" + self.get_master_host() + ":" + NAME_NODE_PORT)
        replicas = c.put_file(sdfs_filename)
        print(replicas)
        c.close()

        content = open(local_filename, "rb").read()
        for replica in replicas:
            c = zerorpc.Client()
            c.connect("tcp://" + replica + ":" + DATA_NODE_PORT)
            c.put_file(sdfs_filename, content)
            c.close()
    
    def get(self, sdfs_filename, local_filename):
        # get address
        c = zerorpc.Client()
        c.connect("tcp://" + self.get_master_host() + ":" + NAME_NODE_PORT)
        replica = c.get_file(sdfs_filename)
        c.close()
        # write to local
        c = zerorpc.Client()
        c.connect("tcp://" + replica + ":" + DATA_NODE_PORT)
        content = c.get_file(sdfs_filename)
        c.close()
        f = open(local_filename, 'wb')
        f.write(content)
    
    def delete(self, sdfs_filename):
        c = zerorpc.Client()
        c.connect("tcp://" + self.get_master_host() + ":" + NAME_NODE_PORT)
        c.delete_file(sdfs_filename)
        c.close()
    
    def ls(self, sdfs_filename):
        c = zerorpc.Client()
        c.connect("tcp://" + self.get_master_host() + ":" + NAME_NODE_PORT)
        print(c.ls(sdfs_filename))
        c.close()

    def store(self, data_node_id):
        c = zerorpc.Client()
        c.connect("tcp://" + self.get_master_host() + ":" + NAME_NODE_PORT)
        c.store(data_node_id)
        c.close()

# test part
c = Client()
c.run()
# c.put("client.py", "test")
# c.get("test", "data_node_cpy")
# c.ls("test")
# c.delete("test")
