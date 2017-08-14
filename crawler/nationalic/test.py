import random
import re
from send_mail import SendEmail


def get_randAccuPos(lis, pos_list):
    gap = random.randint(2, 6)
    slice = sorted(random.sample(lis, gap))
    print(slice)
    pos = 0
    lastpos = 0
    for s in slice:
        if s == 0:
            pos = sum(lis[: int(s)])
        else:
            pos = sum(lis[lastpos: int(s)])
        lastpos = int(s)
        pos_list.append(pos)
    pos = sum(lis[slice[-1]: len(lis) - 1])
    pos_list.append(pos)
    print(pos_list)


if __name__ == "__main__":
    print(random.uniform(16.0, 17.0))
    print(range(2)[1])
    track_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    pos_list = []
    get_randAccuPos(track_list, pos_list)

    total = 16.5
    movesum = 0
    for i in range(3):
        j = random.uniform(5, 10)
        if i != 2:
            movedis = j
            movesum += j

        else:
            movedis = total - movesum

        print(movedis)
    str = "用时0.018秒，查询到1条信息"
    str1 = re.findall("用时(.*)秒，查询到(.*)条信息",
                      str)
    print(str1[0][1])

    myfile = open("D:\\Git\\data\\nationalIcQuery\\companylist.txt")
    lines = len(myfile.readlines())
    print(lines)

    ems = SendEmail('bumpink@126.com', '5178l7220fe', 'cysuncn@126.com', 'nationalIC', 'test', 'smtp.126.com')
    ems.send_email()
