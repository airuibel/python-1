# -*- coding: utf-8 -*-

import time
import uuid
# import StringIO

from PIL import Image
from selenium.webdriver.common.action_chains import ActionChains


class BaseGeetestCrack(object):
    """验证码破解基础类"""

    def __init__(self, driver):
        self.driver = driver
        self.driver.set_window_position(0, 0)
        self.driver.set_window_size(1024, 768)

    def input_by_id(self, text = u"中国移动", element_id = "name"):
        """输入查询关键词

        :text: Unicode, 要输入的文本
        :element_id: 输入框网页元素id

        """
        input_el = self.driver.find_element_by_id(element_id)
        input_el.clear()
        input_el.send_keys(text)
        time.sleep(3.5)

    def click_by_id(self, element_id = "popup-submit"):
        """点击查询按钮

        :element_id: 查询按钮网页元素id

        """
        search_el = self.driver.find_element_by_id(element_id)
        search_el.click()
        time.sleep(3.5)

    def is_pixel_equal(self, img1, img2, x, y):
        pixel1 = img1.getpixel((x, y))
        pixel2 = img2.getpixel((x, y))
        for i in range(0, 3):
            # 如果相差超过50则就认为找到了缺口的位置
            if abs(pixel1[i] - pixel2[i]) >= 50:
                return False
        return True


    def calculate_slider_offset(self):
        """计算滑块偏移位置，必须在点击查询按钮之后调用

        :returns: Number

        """
        img1 = self.crop_captcha_image('img1')
        self.drag_and_drop(x_offset = 5)
        img2 = self.crop_captcha_image('img2')
        w1, h1 = img1.size
        w2, h2 = img2.size
        # print w1, h1, w2, h2
        if w1 != w2 or h1 != h2:
            return False
        left = 0
        flag = False
        for i in range(0, 260):
            for j in range(0, 116):
                if self.is_pixel_equal(img1, img2, i, j) == False:
                    # print i
                    return i


    def crop_captcha_image(self, imagename, element_id = "gt_box"):
        """截取验证码图片

        :element_id: 验证码图片网页元素id
        :returns: StringIO, 图片内容

        """
        captcha_el = self.driver.find_element_by_class_name(element_id)
        location = captcha_el.location
        size = captcha_el.size
        browser_x_offset = 0
        browser_y_offset = 0
        if 'phantomjs' == self.get_browser_name():
            browser_x_offset += 171
            browser_y_offset += 7
        left = int(location['x'] + browser_x_offset)
        top = int(location['y'] + browser_y_offset)
        right = int(location['x'] + browser_x_offset + size['width'])
        bottom = int(location['y'] + browser_y_offset + size['height'])

        screenshot = self.driver.get_screenshot_as_png()

        # screenshot = Image.open(StringIO.StringIO(screenshot))
        captcha = screenshot.crop((left, top, right, bottom))
        captcha.save("%s.png" % imagename)
        return captcha

    def get_browser_name(self):
        """获取当前使用浏览器名称
        :returns: TODO

        """
        return str(self.driver).split('.')[2]

    def drag_and_drop(self, x_offset = 0, y_offset = 0, element_class = "gt_slider_knob"):
        """拖拽滑块

        :x_offset: 相对滑块x坐标偏移
        :y_offset: 相对滑块y坐标偏移
        :element_class: 滑块网页元素CSS类名

        """
        dragger = self.driver.find_element_by_class_name(element_class)
        action = ActionChains(self.driver)
        action.drag_and_drop_by_offset(dragger, x_offset, y_offset).perform()
        # 这个延时必须有，在滑动后等待回复原状
        time.sleep(8)

    def move_to_element(self, element_class = "gt_slider_knob"):
        """鼠标移动到网页元素上

        :element: 目标网页元素

        """
        time.sleep(3)
        element = self.driver.find_element_by_class_name(element_class)
        action = ActionChains(self.driver)
        action.move_to_element(element).perform()
        time.sleep(4.5)

    def crack(self):
        """执行破解程序

        """
        raise NotImplementedError
