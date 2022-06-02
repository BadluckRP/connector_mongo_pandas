import datetime

date = datetime.datetime(2003, 8, 1, 12, 4, 5)

for i in range(5):
    date += datetime.timedelta(days=1)
    print(date)
