from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
import PIL.Image as image
import time, re, random, os
import requests
from retrying import retry

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


def input_by_id(driver, text=u"江苏汇中", element_id="name"):
    """输入查询关键词

    :text: Unicode, 要输入的文本
    :element_id: 输入框网页元素id

    """
    input_el = driver.find_element_by_id(element_id)
    input_el.clear()
    input_el.send_keys(text)
    # time.sleep(3.5)


def click_by_id(driver, element_id="popup-submit"):
    """点击查询按钮

    :element_id: 查询按钮网页元素id

    """
    search_el = driver.find_element_by_id(element_id)
    search_el.click()


def get_fail_signal(driver):  # gt_info_type
    alarm_info = driver.find_element_by_xpath("//div[@class='gt_info_text']/span").text
    #
    print(alarm_info)
    if alarm_info.find(u'验证失败') != -1:
        return True
    else:
        return False


@retry
def do_something_unreliable():
    iedriver = "C:\Program Files\Internet Explorer\IEDriverServer_x64_2.53.1.exe"
    driver = webdriver.Ie(iedriver)  # webdriver.Firefox()
    driver.get("http://www.gsxt.gov.cn/index.html")
    data = "扬中市皇朝家具有限公司"
    input_by_id(driver, text=data, element_id="keyword")
    click_by_id(driver, element_id="btn_query")
    print(driver.find_element_by_xpath("//div[@class='gt_cut_bg gt_show']/div").get_attribute('style'))
    element = driver.find_element_by_xpath("//div[@class='gt_slider_knob gt_show']")
    ActionChains(driver).click_and_hold(on_element=element).perform()
    time.sleep(0.15)
    ActionChains(driver).release(on_element=element).perform()
    WebDriverWait(driver, 30).until(
        lambda the_driver: the_driver.find_element_by_xpath(
            "//div[@class='gt_info_text']").is_displayed()
    )
    print(get_fail_signal(driver))
    if not get_fail_signal(driver):
        time.sleep(3)
        WebDriverWait(driver, 30).until(
            lambda the_driver: the_driver.find_element_by_xpath(
                "//div[@class='search_result g9']").is_displayed()
        )
        str1 = data + '!' + driver.find_element_by_xpath("//div[@class='search_result g9']").text
    else:
        print("验证码不通过")

if __name__ == '__main__':
    do_something_unreliable()



