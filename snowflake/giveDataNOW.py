import random
import datetime
import requests

rows = random.randint(10000+1,100000-1)
# rows = 10**7 # ~ 989 MB

# Way too dank
flatten = (lambda f: lambda v: f(f,v,0))(lambda this, l, pos: (this(this, l[pos], 0) + this(this, l, pos+1)) if (type(l)==list and pos < len(l)) else ([l] if type(l)!=list else []))
simpleParse = lambda s: lambda l,r,flag: ([z.strip() for k,z in enumerate(flatten([x.split(r) for x in ''.join(s).split(l)])) if z.strip()!='' and k%2!=flag])

names_url = "https://www.ssa.gov/oact/babynames/decades/century.html"
nameSet = flatten(list(
    # Schema of x: Row.no(0), Name(1), Weightage(2), Name(3), Weightage(4)
    [x[3],x[1]] for x in (
        # All HTML field-values for every <tr>...</tr>
        simpleParse(tr)('<','>',1) for tr in (
            simpleParse(
                simpleParse(
                    simpleParse(requests.get(names_url).text)('<table class="t-stripe" summary="Popular names for births in 1923-2022">','</table>',0) # Brute-forced this "ID"
                )('<tbody>','</tbody>',0) # Brute-forced this "ID"
            )('<tr align="right">','</tr>',0) # Brute-forced this "ID"
        )
    )
))

with open("data/names.csv", "w") as f:
    f.write("NAMES\n")
    for name in nameSet:
        f.write(name + '\n')

pickOne = lambda s: s[random.randint(0,len(s)-1)]

# Names
names = [pickOne(nameSet) for r in range(rows)]

# ID
ids = [i for i in range(rows)]
random.shuffle(ids)

# Phone numbers
ph_no = lambda prefix: prefix + ''.join(str(random.randint(0,9)) for i in range(10))

phoneBook = set()
_ctr = 0
while(len(phoneBook)!=rows):
    phoneBook.add(ph_no("+91"))
    _ctr += 1
print("Phone numbers generated per record: ",_ctr/rows)

ph_nos = list(phoneBook)

# Email Ids:

# simpleParse-ing not sufficient?
# noun_url = "https://www.talkenglish.com/vocabulary/top-1500-nouns.aspx"

nouns = ['cat','who','expedition','noun','it','dog','rabbit','nix','consumer','product','picture','bread','jail']

domainEnd = lambda: pickOne(['com','net','org','dev','io','cc','to','me'])
domain = (lambda f: lambda: f(f))(lambda this: pickOne(nouns) + '.' + (this(this) if random.random()>0.8 else domainEnd()))

# emails = [names[i].lower() + "%03d"%(random.randint(0,rows)%1000) + '@' + domain() for i in range(rows)]
emails = [names[i].lower() + "%04d"%i + '@' + domain() for i in range(rows)]

# Hire Date:
# referenceTime = datetime.datetime.fromtimestamp(0)

currTime = int(datetime.datetime.now().timestamp())

# 18 years <= age <= 60 years
lowerLimit = 18
upperLimit = 60

pickTime = lambda : random.randint(currTime - (upperLimit-lowerLimit)*365*24*60*60, currTime)
hiredates = [datetime.datetime.fromtimestamp(pickTime()) for i in range(rows)]

# NOTE: upperLimit - yearsHired[i] >= lowerLimit ; for all 'i' ;
yearsHired = [(datetime.datetime.now() - d).days//365 for d in hiredates]

# Age (18 <= age <= 60)
ages = [yearsHired[i] + random.randint(lowerLimit, upperLimit - yearsHired[i]) for i in range(rows)]

# Salary
sal_max = 76914
salaries = [random.randint(100,sal_max) for i in range(rows)]

# Manager
# Q. Are these trees distributed uniformly distributed ? (Assuming randint(a,b) is uniformly distributed)
# A: No, if a company/organisation is an unordered labelled tree.
# These trees are uniformly distributed if and only if assumed to be ordered and labelled.
managers = [-1 for i in range(rows)] # managers[i] = 'id' of manager of i ;

# calcTree():
stack = [(rows-1,0,rows-1-1)]
while(len(stack)!=0):
    rt,l,r = stack.pop()

    if(l>r):
        continue

    x = random.randint(l,r)
    managers[x] = ids[rt]

    stack.append((rt, l, x-1)) # Not rooted at ids[x]
    stack.append((x, x+1, r)) # Rooted at ids[x]


with open("data/bigEmpData.csv", "w") as f:
    f.write("id, name, phone, email, hiredate, age, salary, manager\n")
    for r in sorted(zip(ids,names,ph_nos,emails,hiredates,ages,salaries,managers),key = lambda x: x[0]):
        f.write(', '.join(str(v) for v in r) + '\n')


