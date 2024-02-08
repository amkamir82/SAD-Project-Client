from client import Client
def main():
    cl = Client()
    cl.pull()
    cl.stop()
if __name__ == "__main__":
    main()