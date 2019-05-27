# -*-coding:utf-8 -*-

#如果有斜杠/，分割获取最后一个斜杠/后的字符串
def getlastslashstr( text, slash):
    if(slash in text):
        text = text.split(slash)
        text = text[text.__len__() - 1]
        return text
    else:
        return text

# 如果有斜杠/，分割获第一个斜杠/后的字符串
def getfirstslashstr( text, slash):
    if (slash in text):
        text = text.split(slash)
        text = text[0]
        return text
    else:
        return text