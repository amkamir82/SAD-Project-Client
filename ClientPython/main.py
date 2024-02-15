from client import pull, push, subscribe
lst = []
def on_subscribe(key, value):
    global lst
    print(f'recieved {key},{value} on subscribe')
    lst.append((key,value))
    
def main():
    REMAINING = 30
    for i in range(REMAINING):
         key, val = pull()
         print('pulled: ' , key, val)

    input('check kon okay bashe')


    KEY_RANGE = 30
    KEYS = 10
    push_seq = { _ : [] for _ in range(KEYS)}
    for i in range(KEY_RANGE):
        print(f"pushing {i} with key {i%KEYS}")
        push(f"{i%KEYS}", i)
        push_seq[i%KEYS].append(i)

    pull_seq = { _ : [] for _ in range(KEYS)}
    for i in range(KEY_RANGE):
        key, val = pull()
        pull_seq[int(key)].append(int(val))

    for i in range(REMAINING):
        key, val = pull()
        print(key, val)

    print(f'push_seq', push_seq)
    print(f'pull_seq', pull_seq)
    input('is it ok?')


    SUBSCRIBERS = 5
    KEY_RANGE = 30
    KEYS = 10
    for i in range(SUBSCRIBERS):
        subscribe(on_subscribe)
        
    push_seq = { _ : [] for _ in range(KEYS)}
    for i in range(KEYS):
        print(f"[*] pushing {i} with key {i%KEYS}")
        push(f"{i%KEYS}", i)
        push_seq[i%KEYS].append(i)
    input('is subscribe working?')
    print(lst)
    input('how is it?')
    
if __name__ == "__main__":
    main()
