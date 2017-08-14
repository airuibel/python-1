#-*- coding:utf-8 -*-
from selenium import webdriver
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
iedriver = "C:\Program Files\Internet Explorer\IEDriverServer.exe"
#iedriver = "C:\Program Files\Internet Explorer\IEDriverServer_x64_2.53.1.exe"
os.environ["webdriver.ie.driver"] = iedriver
#driver = webdriver.Chrome()
driver = webdriver.Ie(iedriver) #webdriver.Firefox()
urlLogin='http://66.143.97.152/mydomain/login.do?method=begin'
driver.get(urlLogin)
#time.sleep(1)
# fill in account and password
elemUser = driver.find_element_by_name("j_username")
elemUser.clear()
userstr="bank"
elemUser.send_keys(userstr)
#elemUser.send_keys("bank")
elemPasswd = driver.find_element_by_name("j_password").send_keys("bank")
#elemPasswd.send_keys("bank")
# click the button of login
elemclick = driver.find_element_by_name("Image1").click()
#elemclick.click()
#print driver.title
urlForPage = 'http://66.143.97.152/mydomain/estate/pages/bankSelect/pages/bankSelect.jsp?menuName=1307432674550'
driver.get(urlForPage)
elemTextbox = driver.find_element_by_name("syqzh")
elemTextbox.clear()
elemTextbox.send_keys("140003942")
#elemTextbox.send_keys("140003942") #71003374
elemclick = driver.find_element_by_class_name("button2").click()
#elemclick.click()
time.sleep(3)
html = driver.execute_script("return document.documentElement.outerHTML")
print (html)
elemTextbox = driver.find_element_by_name("syqzh")
elemTextbox.clear()
elemTextbox.send_keys("71003374")
#elemTextbox.send_keys("140003942") #71003374
elemclick = driver.find_element_by_class_name("button2").click()
#elemclick.click()
time.sleep(3)
html = driver.execute_script("return document.documentElement.outerHTML")
print (html)
driver.quit()
