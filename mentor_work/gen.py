import random
import sys

HASH_BITS = 10
sys.set_int_max_str_digits(10**9)
sys.setrecursionlimit(10**9)
# coinflip = lambda: 1 if (...) else 0

# NOTE: hash() gives distinct values for identical input on different process instances !!!

def abstract(elem) -> (int,int):

    # Order-Independent
    if (hasattr(elem,'__getitem__')):
        elem = ''.join((f"%0100d" % e) for e in sorted(elem))

    # No '-' symbol!
    ID = hash(str(elem))%(1 << HASH_BITS)
    VAL = 0 if elem == '' else 1 + int(elem)%998244353

    return (ID, VAL)


ID, VALUE = abstract(1)
QUANTITY = 10**3

assert (0,0) == abstract('')

UNIVERSE = { 0: {'VALUE':0, 'QUANTITY': QUANTITY}, ID: {'VALUE':VALUE, 'QUANTITY': QUANTITY} }
SIZE = QUANTITY + QUANTITY


def create() -> (list, int):
    global UNIVERSE
    global SIZE

    # IndexError: list index out of range ?? Where is this bug in choose() ?
    def choose(ids,size):
        global UNIVERSE

        if (random.randint(1,size) <= UNIVERSE[ids[len(ids)-1]]['QUANTITY']):
            return ids[len(ids)-1]
        else:
            return choose(ids, size - UNIVERSE[ids.pop()]['QUANTITY'])


    ELEM = [choose(list(UNIVERSE),SIZE)]
    WORTH = UNIVERSE[ELEM[len(ELEM)-1]]['VALUE']
    while(ELEM[len(ELEM)-1]!=0):
        SIZE -= 1

        UNIVERSE[ELEM[len(ELEM)-1]]['QUANTITY'] -= 1
        WORTH += UNIVERSE[ELEM[len(ELEM)-1]]['VALUE']

        ELEM.append(choose(list(UNIVERSE),SIZE))

    ELEM.pop()
    return (ELEM, WORTH)


TIME = 5*10**4

while (TIME > 0):
    TIME -= 1

    ELEM, WORTH = create()
    ID, VAL = abstract(ELEM)

    # UNIVERSE loses WORTH ?
    if ID == 0:
        # if VAL != 0:
        #     print(ID, ELEM, VAL, WORTH)
        continue

    # VAL*UNITS == WORTH ; type(UNITS) == int ; UNITS >= 1 ;
    UNITS = int(WORTH//VAL + (WORTH%VAL!=0))
    VAL = WORTH/UNITS

    # if(VAL*UNITS != WORTH):
    #     print("ERROR:",WORTH-VAL*UNITS)

    if (UNIVERSE.get(ID,None)!=None):
        V,Q = UNIVERSE[ID]['VALUE'], UNIVERSE[ID]['QUANTITY']

        UNIVERSE[ID]['VALUE'] = (V*Q + WORTH)/(Q + UNITS)
        UNIVERSE[ID]['QUANTITY'] = (Q + UNITS)

    else:
        UNIVERSE[ID] = {'VALUE': VAL, 'QUANTITY': UNITS }

    SIZE += UNITS

    # Why is BALANCE continuously increasing?
    BALANCE = sum(V['VALUE']*V['QUANTITY'] for K,V in UNIVERSE.items())
    print("Size:",SIZE,"Uniqueness:",len(UNIVERSE),"Balance:",BALANCE,end='\r')

# for K,V in UNIVERSE.items():
#     print("ID:",K,end=' | ')
#     print("VALUE:",V["VALUE"],"QUANTITY:",V["QUANTITY"],end=' | ')
#     print("TOTAL:",V["VALUE"]*V["QUANTITY"])

print("Fin.")
