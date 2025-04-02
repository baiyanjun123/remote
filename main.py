# coding=gbk
import csv
import os
import ftplib
from datetime import datetime
import time

def getHourStr(hourNum, minNum):
    if minNum < 35:
        hourNum -=1
    hourStr = str(hourNum)
    if hourNum < 10:
        hourStr = "0" + str(hourNum)
    else:
        hourStr = str(hourNum)
    return hourStr

# 这个函数用于将日期从整型转为FTP路径所需的字符串
def getDateStr(yearNum, monNum, dayNum):
    # 四位数年份
    yearStr = str(yearNum)

    # 两位数月份
    if monNum < 10:
        monStr = "0" + str(monNum)
    else:
        monStr = str(monNum)

    # 两位数天
    if dayNum < 10:
        dayStr = "0" + str(dayNum)
    else:
        dayStr = str(dayNum)

    return yearStr, monStr, dayStr

# 这个函数用于在跨月时获取前一天的日期号
def getYesterday(year, month, day):
    if day == 1:

        if month == 1:
            year -= 1
            month = 12
            day = 31

        elif month == 2 or month == 4 or month == 6 or month == 8 or month == 9 or month == 11:
            month -= 1
            day = 31

        elif month == 5 or month == 7 or month == 10 or month == 12:
            month -= 1
            day = 30

        elif month == 3:
            # 闰年
            if year % 4 == 0 and year % 400 == 0:
                day = 29
                month -= 1
            # 闰年
            elif year % 4 == 0 and year % 100 != 0:
                day = 29
                month -= 1
            else:
                day = 28
                month -= 1
    else:
        day -= 1

    return year, month, day

# 获取文件后缀名
def suffix(file, *suffixName):
    array = map(file.endswith, suffixName)
    if True in array:
        return True
    else:
        return False

# 删除目录下扩展名为.temp的文件
def deleteFile(fileDir):
    if not os.path.exists(fileDir):
        os.makedirs(fileDir)
    targetDir = fileDir
    for file in os.listdir(targetDir):
        targetFile = os.path.join(targetDir, file)
        if suffix(file, '.temp'):
            os.remove(targetFile)

class myFTP:
    ftp = ftplib.FTP()

    # 连接FTP，host是IP地址，port是端口，默认21
    def __init__(self, host, port=21):
        self.ftp.connect(host, port)

    # 登录FTP连接，user是用户名，password是密码
    def Login(self, user, password):
        self.ftp.login(user, password)
        print(self.ftp.welcome)  # 显示登录信息

    # 下载单个文件，LocalFile表示本地存储路径和文件名，RemoteFile是FTP路径和文件名
    def DownLoadFile(self, LocalFile, RemoteFile):
        bufSize = 102400

        file_handler = open(LocalFile, 'wb')
        print(file_handler)

        # 接收服务器上文件并写入本地文件
        self.ftp.retrbinary('RETR ' + RemoteFile, file_handler.write, bufSize)
        self.ftp.set_debuglevel(0)
        file_handler.close()
        return True

    # 下载整个目录下的文件，LocalDir表示本地存储路径， emoteDir表示FTP路径
    def DownLoadFileTree_FirstTime(self, LocalDir, RemoteDir, choice):
        # print("remoteDir:", RemoteDir)
        # 如果本地不存在该路径，则创建
        if not os.path.exists(LocalDir):
            os.makedirs(LocalDir)

        # 获取FTP路径下的全部文件名，以列表存储
        # 好像是乱序
        self.ftp.cwd(RemoteDir)
        RemoteNames = self.ftp.nlst()
        print(RemoteNames)
        RemoteNames.reverse()

        # print("RemoteNames：", RemoteNames)
        for file in RemoteNames:
            # 防止上一次下载中断后，最后一个下载的文件未下载完整，而再开始下载时，程序会识别为已经下载完成
            Local = os.path.join(LocalDir, file[0:-4] + ".temp")
            LocalNew = os.path.join(LocalDir, file)

            # 若已经存在，则跳过下载
            # 小时数据命名格式示例:
            # H08_20201102_2350_L2WLFbet_FLDK.06001_06001.csv
            # 创建文件夹保存数据
            if choice == 1:
                if not os.path.exists(LocalNew):
                    print("下载文件 %s 中" % file)
                    self.DownLoadFile(Local, file)
                    os.rename(Local, LocalNew)
                    print("文件 %s 下载完成\n" % file)
                elif os.path.exists(LocalNew):
                    print("文件 %s 已存在!\n" % file)

        self.ftp.cwd("..")
        return
    def DownLoadFileTree(self, LocalDir, RemoteDir, choice, _yearStr, _monStr):
        # print("remoteDir:", RemoteDir)
        # 如果本地不存在该路径，则创建
        if not os.path.exists(LocalDir):
            os.makedirs(LocalDir)

        # 获取FTP路径下的全部文件名，以列表存储
        # 好像是乱序
        self.ftp.cwd(RemoteDir)
        RemoteNames = self.ftp.nlst()
        print(RemoteNames)
        RemoteNames.reverse()

        # print("RemoteNames：", RemoteNames)
        for file in RemoteNames:
            # 防止上一次下载中断后，最后一个下载的文件未下载完整，而再开始下载时，程序会识别为已经下载完成
            Local = os.path.join(LocalDir, file[0:-4] + ".temp")
            LocalNew = os.path.join(LocalDir, file)

            # 若已经存在，则跳过下载
            # 小时数据命名格式示例:
            # H08_20201102_2350_L2WLFbet_FLDK.06001_06001.csv
            # 创建文件夹保存数据
            if choice == 1:
                if not os.path.exists(LocalNew):
                    print("下载文件 %s 中" % file)
                    self.DownLoadFile(Local, file)
                    os.rename(Local, LocalNew)
                    print("文件 %s 下载完成\n" % file)

        self.ftp.cwd("..")
        return

    def close(self):
        self.ftp.quit()

if __name__ == "__main__":
    utcdt = datetime.utcnow()
    _yearNum = '20' + utcdt.strftime('%y')
    _monNum = utcdt.strftime('%m')
    _dayNum = utcdt.strftime('%d')
    _hourNum = utcdt.strftime('%H')
    _minNum = utcdt.strftime('%M')
    _yearStr = ""
    _monStr = ""
    _dayStr = ""
    _hourStr = ""
    print("当前utc时间是:", utcdt)
    # 传入IP地址
    ftp = myFTP('ftp.ptree.jaxa.jp')
    # 传入用户名和密码
    ftp.Login('350021908_qq.com', 'SP+wari8')
    _yearStr, _monStr, _dayStr = getDateStr(int(_yearNum), int(_monNum), int(_dayNum))
    _hourStr = getHourStr(int(_hourNum), int(_minNum))
    # 从目标路径ftp_filePath将文件下载至本地路径dst_filePath
    dst_filePath = "C:/kuihua"
    deleteFile(dst_filePath)  # 先删除存储路径中的临时文件（也就是上次未下载完整的文件）
    ftp_filePath = "/pub/himawari/L2/WLF/010" + "/" + _yearStr + _monStr + "/" + _dayStr + "/" + _hourStr
    print("当前下载时间:", utcdt)
    try:
        ftp.DownLoadFileTree(dst_filePath, ftp_filePath, 1, _yearStr, _monStr)
    except Exception as e:
        print(e)
    ftp.close()
    print("下载完成，开始扫描文件夹导入数据 ")
    os.chdir(dst_filePath)  # 路径设置成csv文件放的地方
    path = os.getcwd()
    files = os.listdir(path)
    print("当前文件已全部导入 ")

    while True:
        utcdt = datetime.utcnow()
        _yearNum = '20' + utcdt.strftime('%y')
        _monNum = utcdt.strftime('%m')
        _dayNum = utcdt.strftime('%d')
        _hourNum = utcdt.strftime('%H')
        _minNum = utcdt.strftime('%M')
        _yearStr = ""
        _monStr = ""
        _dayStr = ""
        _hourStr = ""
        print("当前utc时间是:",  utcdt)
        # 传入IP地址
        ftp = myFTP('ftp.ptree.jaxa.jp')
        # 传入用户名和密码
        ftp.Login('350021908_qq.com', 'SP+wari8')
        _yearStr, _monStr, _dayStr = getDateStr(int(_yearNum), int(_monNum), int(_dayNum))
        _hourStr = getHourStr(int(_hourNum), int(_minNum))
        # 从目标路径ftp_filePath将文件下载至本地路径dst_filePath
        dst_filePath = "C:/kuihua"
        deleteFile(dst_filePath)  # 先删除存储路径中的临时文件（也就是上次未下载完整的文件）
        ftp_filePath = "/pub/himawari/L2/WLF/010" + "/" + _yearStr + _monStr + "/" + _dayStr + "/" + _hourStr
        print("当前下载时间:",utcdt)
        try:
            ftp.DownLoadFileTree(dst_filePath, ftp_filePath, 1, _yearStr, _monStr)
        except Exception as e:
            print(e)
        # 结束
        ftp.close()
        print("下载与导入完成，等待下次数据更新 ")
        if int(_minNum) % 10 < 6:
            time.sleep(60 * (6 - (int(_minNum) % 10)))
        else:
            time.sleep(60 * (16 - (int(_minNum) % 10)))
