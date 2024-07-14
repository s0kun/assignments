t = int(input())
while t>0:
    t -= 1

    n = int(input())
    a = list(map(int,input().split()))
    b = list(map(int,input().split()))

    score_a = score_b = 0
    ctr_n = ctr_p = 0
    for i in range(n):
        if a[i] == b[i]:
            ctr_n += (a[i]==-1)
            ctr_p += (b[i]==1)
        elif a[i] > b[i]:
            score_a += a[i]
        else:
            score_b += b[i]

    if score_a > score_b:
        score_a, score_b = score_b, score_a
    # print("Scores:",score_b,score_a)
    # print("Rem:",ctr_p,-ctr_n)

    if score_a < score_b:
        T = min(ctr_p, score_b-score_a)
        score_a += T
        ctr_p -= T

    if ctr_p != 0:
        score_a += ctr_p>>1
        score_b += (ctr_p+1)>>1

    if score_a < score_b:
        T = min(ctr_n, score_b-score_a)
        score_b -= T
        ctr_n -= T

    if ctr_n != 0:
        score_a -= ctr_n>>1
        score_b -= (ctr_n+1)>>1

    print(min(score_b,score_a))

# Test Case:
# 4
# 2
# -1 1
# -1 -1
# 1
# -1
# -1
# 5
# 0 -1 1 0 1
# -1 1 0 0 1
# 4
# -1 -1 -1 1
# -1 1 1 1

# Output:
# 0
# -1
# 1
# 1

