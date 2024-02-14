from client import pull, push, subscribe

def main():
    print('[+] sending arshia1')
    push('arshia', 'akhavan1')
    print('[+] sending arshia2')
    push('arshia2', 'akhavan2')
    print('[+] sending arshia3')
    push('arshia3', 'akhavan3')
    print('[+] sending arshia4')
    push('arshia4', 'akhavan4')
    print('[+] sending arshia5')
    push('arshia5', 'akhavan5')
    print('[+] sending arshia6')
    push('arshia6', 'akhavan6')

    print(pull())
    print(pull())
    print(pull())
    print(pull())
    print(pull())
    print(pull())
    input('check kon okay bashe')


if __name__ == "__main__":
    main()
