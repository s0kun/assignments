with open("sales.csv","r") as f:
    data = f.readlines()
    headers,sales = data[0], data[1:len(data)]

limit = ...
for i in range(20):
    sales = sales + ["\n"] + sales

with open("bigSales.csv","w") as f:
    f.write(''.join(headers))
    f.write(''.join(sales))