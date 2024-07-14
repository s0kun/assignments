# Given a string 's' of length 'n' 0 <= n <= 5000
# s[i] belongs to {'a', ..., 'z', '?'}
# Find a
t = int(input())
while t>0:
    t -= 1

    s = input()
    n = len(s)
    mx = 0
    for i in range(1,1 + n//2):
        # flags = [False for k in range(i)]
        ctr = 0
        # pos = 0

        # print(f"\n i = {i}")
        for j in range(i,n):
            # ctr -= flags[(pos-i)%i]
            # flags[pos] = ((s[j] == '?') or (s[j-i] == '?') or (s[j] == s[j-i]))
            # ctr += flags[pos]
            if not ((s[j] == '?') or (s[j-i] == '?') or (s[j] == s[j-i])):
                ctr = 0
                continue
            ctr += 1

            if ctr == i:
                mx = i<<1
                break
            # pos = (pos+1)%i

            # print(f"flags: {flags} ctr: {ctr} pos: {pos} j: {s[j]}")

    print(mx)

# Test case: ?c?dbb
# Answer : 2
