from client import pull, push, subscribe

def main():
    
    push('arshia', 'akhavan')
    push('arshia2', 'akhavan1')
    push('arshia3', 'akhavan2')
    push('arshia4', 'akhavan3')
    
    print(pull())


    subscribe(print)
    input()


if __name__ == "__main__":
    main()
