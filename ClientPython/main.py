from client import pull, push, subscribe

def main():
    
    # push('arshia', 'akhavan1')
    # push('arshia2', 'akhavan2')
    # push('arshia3', 'akhavan3')
    # push('arshia4', 'akhavan4')
    # push('arshia5', 'akhavan5')
    push('arshia6', 'akhavan6')

    subscribe(print)
    
    input()
    subscribe(print)
    input()


if __name__ == "__main__":
    main()
