import zerorpc

class Client:
    def __init__(self):
        pass
    
    def get_master_host(self):
        return "192.0.0.1"
    
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
        c.connect("tcp://127.0.0.1:4241")
        data_node_address = c.put_file(sdfs_filename)
        c.close()
        content = open(local_filename, "rb").read()
        for address in data_node_address:
            c = zerorpc.Client()
            c.connect("tcp://" + address + ":4242")
            c.put_file(sdfs_filename, content)
            c.close()
    
    def get(self, sdfs_filename, local_filename):
        # get address
        c = zerorpc.Client()
        c.connect("tcp://127.0.0.1:4241")
        data_node_address = c.get_file(sdfs_filename)
        c.close()
        # write to local
        c = zerorpc.Client()
        c.connect("tcp://" + data_node_address + ":4242")
        content = c.get_file(sdfs_filename)
        c.close()
        f = open(local_filename, 'wb')
        f.write(content)
    
    def delete(self, sdfs_filename):
        c = zerorpc.Client()
        c.connect("tcp://127.0.0.1:4241")
        c.delete_file(sdfs_filename)
        c.close()
    
    def ls(self, sdfs_filename):
        c = zerorpc.Client()
        c.connect("tcp://127.0.0.1:4241")
        print(c.ls(sdfs_filename))
        c.close()

    def store(self, data_node_id):
        c = zerorpc.Client()
        c.connect("tcp://127.0.0.1:4241")
        c.store(data_node_id)
        c.close()

# test part
c = Client()
c.run()
# c.put("client.py", "test")
# c.get("test", "data_node_cpy")
# c.ls("test")
# c.delete("test")
