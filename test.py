from datetime import datetime

if __name__ == '__main__':
    str_startdate = "2021/09/17"
    start_date = datetime.strptime(str_startdate, "%Y/%m/%d")
    presentTime = datetime.now()
    if presentTime.date() >= start_date.date():
        print("TRUE!!")
    else :
        print("FALSE!!")
    