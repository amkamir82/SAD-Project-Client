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

    
    KEY_RANGE = 100
    KEYS = 10
    push_seq = { _ : [] for _ in range(KEYS)}
    for i in range(KEY_RANGE):
        print(f"pushing {i} with key {i%KEYS}")
        push(f"{i%KEYS}", i)
        push_seq[i].append(i)

    pull_seq = { _ : [] for _ in range(KEYS)}
    for i in range(KEY_RANGE):
        key, val = pull()
        pull_seq[key].append(val)
        
    print(f'push_seq', push_seq)
    print(f'pull_seq', pull_seq)
    input('is it ok?')
    
    
    KEY_RANGE = 100
    KEYS = 10
    for i in range(KEY_RANGE):
        subscribe(print)
    push_seq = { _ : [] for _ in range(KEYS)}
    for i in range(KEY_RANGE):
        print(f"pushing {i} with key {i%KEYS}")
        push(f"{i%KEYS}", i)
        push_seq[i].append(i)
    
    print('is subscribe working?')
    
if __name__ == "__main__":
    main()
