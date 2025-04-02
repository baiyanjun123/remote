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

# ����������ڽ����ڴ�����תΪFTP·��������ַ���
def getDateStr(yearNum, monNum, dayNum):
    # ��λ�����
    yearStr = str(yearNum)

    # ��λ���·�
    if monNum < 10:
        monStr = "0" + str(monNum)
    else:
        monStr = str(monNum)

    # ��λ����
    if dayNum < 10:
        dayStr = "0" + str(dayNum)
    else:
        dayStr = str(dayNum)

    return yearStr, monStr, dayStr

# ������������ڿ���ʱ��ȡǰһ������ں�
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
            # ����
            if year % 4 == 0 and year % 400 == 0:
                day = 29
                month -= 1
            # ����
            elif year % 4 == 0 and year % 100 != 0:
                day = 29
                month -= 1
            else:
                day = 28
                month -= 1
    else:
        day -= 1

    return year, month, day

# ��ȡ�ļ���׺��
def suffix(file, *suffixName):
    array = map(file.endswith, suffixName)
    if True in array:
        return True
    else:
        return False

# ɾ��Ŀ¼����չ��Ϊ.temp���ļ�
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

    # ����FTP��host��IP��ַ��port�Ƕ˿ڣ�Ĭ��21
    def __init__(self, host, port=21):
        self.ftp.connect(host, port)

    # ��¼FTP���ӣ�user���û�����password������
    def Login(self, user, password):
        self.ftp.login(user, password)
        print(self.ftp.welcome)  # ��ʾ��¼��Ϣ

    # ���ص����ļ���LocalFile��ʾ���ش洢·�����ļ�����RemoteFile��FTP·�����ļ���
    def DownLoadFile(self, LocalFile, RemoteFile):
        bufSize = 102400

        file_handler = open(LocalFile, 'wb')
        print(file_handler)

        # ���շ��������ļ���д�뱾���ļ�
        self.ftp.retrbinary('RETR ' + RemoteFile, file_handler.write, bufSize)
        self.ftp.set_debuglevel(0)
        file_handler.close()
        return True

    # ��������Ŀ¼�µ��ļ���LocalDir��ʾ���ش洢·���� emoteDir��ʾFTP·��
    def DownLoadFileTree_FirstTime(self, LocalDir, RemoteDir, choice):
        # print("remoteDir:", RemoteDir)
        # ������ز����ڸ�·�����򴴽�
        if not os.path.exists(LocalDir):
            os.makedirs(LocalDir)

        # ��ȡFTP·���µ�ȫ���ļ��������б�洢
        # ����������
        self.ftp.cwd(RemoteDir)
        RemoteNames = self.ftp.nlst()
        print(RemoteNames)
        RemoteNames.reverse()

        # print("RemoteNames��", RemoteNames)
        for file in RemoteNames:
            # ��ֹ��һ�������жϺ����һ�����ص��ļ�δ�������������ٿ�ʼ����ʱ�������ʶ��Ϊ�Ѿ��������
            Local = os.path.join(LocalDir, file[0:-4] + ".temp")
            LocalNew = os.path.join(LocalDir, file)

            # ���Ѿ����ڣ�����������
            # Сʱ����������ʽʾ��:
            # H08_20201102_2350_L2WLFbet_FLDK.06001_06001.csv
            # �����ļ��б�������
            if choice == 1:
                if not os.path.exists(LocalNew):
                    print("�����ļ� %s ��" % file)
                    self.DownLoadFile(Local, file)
                    os.rename(Local, LocalNew)
                    print("�ļ� %s �������\n" % file)
                elif os.path.exists(LocalNew):
                    print("�ļ� %s �Ѵ���!\n" % file)

        self.ftp.cwd("..")
        return
    def DownLoadFileTree(self, LocalDir, RemoteDir, choice, _yearStr, _monStr):
        # print("remoteDir:", RemoteDir)
        # ������ز����ڸ�·�����򴴽�
        if not os.path.exists(LocalDir):
            os.makedirs(LocalDir)

        # ��ȡFTP·���µ�ȫ���ļ��������б�洢
        # ����������
        self.ftp.cwd(RemoteDir)
        RemoteNames = self.ftp.nlst()
        print(RemoteNames)
        RemoteNames.reverse()

        # print("RemoteNames��", RemoteNames)
        for file in RemoteNames:
            # ��ֹ��һ�������жϺ����һ�����ص��ļ�δ�������������ٿ�ʼ����ʱ�������ʶ��Ϊ�Ѿ��������
            Local = os.path.join(LocalDir, file[0:-4] + ".temp")
            LocalNew = os.path.join(LocalDir, file)

            # ���Ѿ����ڣ�����������
            # Сʱ����������ʽʾ��:
            # H08_20201102_2350_L2WLFbet_FLDK.06001_06001.csv
            # �����ļ��б�������
            if choice == 1:
                if not os.path.exists(LocalNew):
                    print("�����ļ� %s ��" % file)
                    self.DownLoadFile(Local, file)
                    os.rename(Local, LocalNew)
                    print("�ļ� %s �������\n" % file)

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
    print("��ǰutcʱ����:", utcdt)
    # ����IP��ַ
    ftp = myFTP('ftp.ptree.jaxa.jp')
    # �����û���������
    ftp.Login('350021908_qq.com', 'SP+wari8')
    _yearStr, _monStr, _dayStr = getDateStr(int(_yearNum), int(_monNum), int(_dayNum))
    _hourStr = getHourStr(int(_hourNum), int(_minNum))
    # ��Ŀ��·��ftp_filePath���ļ�����������·��dst_filePath
    dst_filePath = "C:/kuihua"
    deleteFile(dst_filePath)  # ��ɾ���洢·���е���ʱ�ļ���Ҳ�����ϴ�δ�����������ļ���
    ftp_filePath = "/pub/himawari/L2/WLF/010" + "/" + _yearStr + _monStr + "/" + _dayStr + "/" + _hourStr
    print("��ǰ����ʱ��:", utcdt)
    try:
        ftp.DownLoadFileTree(dst_filePath, ftp_filePath, 1, _yearStr, _monStr)
    except Exception as e:
        print(e)
    ftp.close()
    print("������ɣ���ʼɨ���ļ��е������� ")
    os.chdir(dst_filePath)  # ·�����ó�csv�ļ��ŵĵط�
    path = os.getcwd()
    files = os.listdir(path)
    print("��ǰ�ļ���ȫ������ ")

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
        print("��ǰutcʱ����:",  utcdt)
        # ����IP��ַ
        ftp = myFTP('ftp.ptree.jaxa.jp')
        # �����û���������
        ftp.Login('350021908_qq.com', 'SP+wari8')
        _yearStr, _monStr, _dayStr = getDateStr(int(_yearNum), int(_monNum), int(_dayNum))
        _hourStr = getHourStr(int(_hourNum), int(_minNum))
        # ��Ŀ��·��ftp_filePath���ļ�����������·��dst_filePath
        dst_filePath = "C:/kuihua"
        deleteFile(dst_filePath)  # ��ɾ���洢·���е���ʱ�ļ���Ҳ�����ϴ�δ�����������ļ���
        ftp_filePath = "/pub/himawari/L2/WLF/010" + "/" + _yearStr + _monStr + "/" + _dayStr + "/" + _hourStr
        print("��ǰ����ʱ��:",utcdt)
        try:
            ftp.DownLoadFileTree(dst_filePath, ftp_filePath, 1, _yearStr, _monStr)
        except Exception as e:
            print(e)
        # ����
        ftp.close()
        print("�����뵼����ɣ��ȴ��´����ݸ��� ")
        if int(_minNum) % 10 < 6:
            time.sleep(60 * (6 - (int(_minNum) % 10)))
        else:
            time.sleep(60 * (16 - (int(_minNum) % 10)))
