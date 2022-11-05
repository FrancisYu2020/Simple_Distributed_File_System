import threading

class testFD:
    def __init__(self) -> None:
        self.ML = ["fa22-cs425-2201.cs.illinois.edu", "fa22-cs425-2202.cs.illinois.edu",
                "fa22-cs425-2203.cs.illinois.edu", "fa22-cs425-2204.cs.illinois.edu",
                "fa22-cs425-2205.cs.illinois.edu"]
        self.all = ["fa22-cs425-2201.cs.illinois.edu", "fa22-cs425-2202.cs.illinois.edu",
                "fa22-cs425-2203.cs.illinois.edu", "fa22-cs425-2204.cs.illinois.edu",
                "fa22-cs425-2205.cs.illinois.edu"]
    
    def join(self, order):
        if self.all[order] not in self.ML:
            self.ML.append(order)
    
    def leave(self, order):
        if self.all[order] in self.ML:
            self.ML.remove(self.all[order])

    def run(self):
        while 1:
            print("Please input command:")
            command = input()
            args = command.split(" ")
            command = args[0]
            if command == "list_mem":
                print(self.ML)
            elif command == "join":
                self.join(args[1])
            elif command == "leave":
                self.leave(args[1])
            elif command == "help":
                print("=======================================================")
                print("1. list_mem: list current membership list in the ring")
                print("2. list_nb: list current neighbor list in current node")
                print("3. join: join current node to the ring")
                print("4. leave: leave current node from the ring")
                print("5. help: show this usage prompt")
                print("6. exit: shutdown this node")
                print("=======================================================")
            elif command == "exit":
                EXIT_SIGNAL = 1
                self.sucide()
                return
            else:
                print("invalid command, use help to check available commands!")
            