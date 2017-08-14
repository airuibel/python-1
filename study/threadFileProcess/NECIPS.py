# -*- coding: utf-8 -*-
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
import PIL.Image as image
import time, re, random, os
import requests
import logging
import logging.handlers
from retrying import retry
from send_mail import SendEmail

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

# 爬虫模拟的浏览器头部信息
agent = 'Mozilla/5.0 (Windows NT 5.1; rv:33.0) Gecko/20100101 Firefox/33.0'
headers = {
    'User-Agent': agent
}


class NECIPS(object):
    def __init__(self, src_name, done_name, tmp_dir, dest_name, log_name):
        self.src_name = src_name
        self.done_name = done_name
        self.tmp_dir = tmp_dir
        self.dest_name = dest_name
        self.log_name = log_name
        handler = logging.handlers.RotatingFileHandler(self.log_name, maxBytes = 1024 * 1024,
                                                       backupCount = 5)  # 实例化handler
        fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
        formatter = logging.Formatter(fmt)  # 实例化formatter
        handler.setFormatter(formatter)  # 为handler添加formatter
        self.logger = logging.getLogger('tst')  # 获取名为tst的logger
        self.logger.addHandler(handler)  # 为logger添加handler
        self.logger.setLevel(logging.DEBUG)

    # 根据位置对图片进行合并还原
    def get_merge_image(self, filename, location_list):
        # 打开图片文件
        im = image.open(self.tmp_dir + filename)
        # 创建新的图片,大小为260*116
        new_im = image.new('RGB', (260, 116))
        im_list_upper = []
        im_list_down = []
        # 拷贝图片
        for location in location_list:
            # 上面的图片
            if location['y'] == -58:
                im_list_upper.append(im.crop((abs(location['x']), 58, abs(location['x']) + 10, 166)))
            # 下面的图片
            if location['y'] == 0:
                im_list_down.append(im.crop((abs(location['x']), 0, abs(location['x']) + 10, 58)))
        new_im = image.new('RGB', (260, 116))
        x_offset = 0
        # 黏贴图片
        for im in im_list_upper:
            new_im.paste(im, (x_offset, 0))
            x_offset += im.size[0]
        x_offset = 0
        for im in im_list_down:
            new_im.paste(im, (x_offset, 58))
            x_offset += im.size[0]
        return new_im

    # 下载并还原图片
    # driver:webdriver
    # div:图片的div
    def get_image(self, driver, div):
        # 找到图片所在的div
        background_images = driver.find_elements_by_xpath(div)
        location_list = []
        imageurl = ''
        # 图片是被CSS按照位移的方式打乱的,我们需要找出这些位移,为后续还原做好准备
        for background_image in background_images:
            location = {}
            # 在html里面解析出小图片的url地址，还有长高的数值
            location['x'] = int(re.findall("background-position: (.*)px (.*)px; background-image: url\(\"(.*)\"\);",
                                           background_image.get_attribute('style'))[0][0])
            location['y'] = int(re.findall("background-position: (.*)px (.*)px; background-image: url\(\"(.*)\"\);",
                                           background_image.get_attribute('style'))[0][1])
            imageurl = re.findall("background-position: (.*)px (.*)px; background-image: url\(\"(.*)\"\);",
                                  background_image.get_attribute('style'))[0][2]
            location_list.append(location)
        # 替换图片的后缀,获得图片的URL
        imageurl = imageurl.replace("webp", "jpg")
        # 获得图片的名字
        imageName = imageurl.split('/')[-1]
        # 获得图片
        session = requests.session()
        r = session.get(imageurl, headers = headers, verify = False)
        # 下载图片
        with open(self.tmp_dir + imageName, 'wb') as f:
            f.write(r.content)
            f.close()
        # 重新合并还原图片
        image = self.get_merge_image(imageName, location_list)
        return image

    # 对比RGB值
    def is_similar(self, image1, image2, x, y):
        pass
        # 获取指定位置的RGB值
        pixel1 = image1.getpixel((x, y))
        pixel2 = image2.getpixel((x, y))
        for i in range(0, 3):
            # 如果相差超过50则就认为找到了缺口的位置
            if abs(pixel1[i] - pixel2[i]) >= 50:
                return False
        return True

    # 计算缺口的位置
    def get_diff_location(self, image1, image2):
        i = 0
        # 两张原始图的大小都是相同的260*116
        # 那就通过两个for循环依次对比每个像素点的RGB值
        # 如果相差超过50则就认为找到了缺口的位置
        for i in range(0, 260):
            for j in range(0, 116):
                if not self.is_similar(image1, image2, i, j):
                    return i

    # 根据缺口的位置模拟x轴移动的轨迹
    def get_track(self, length):
        pass
        list = []
        # 间隔通过随机范围函数来获得,每次移动一步或者两步
        x = random.randint(1, 3)
        # 生成轨迹并保存到list内
        while length - x >= 5:
            list.append(x)
            length = length - x
            x = random.randint(1, 3)
        # 最后五步都是一步步移动
        for i in range(length):
            list.append(1)
        return list

    def input_by_id(self, driver, text = u"江苏汇中", element_id = "name"):
        """输入查询关键词

        :text: Unicode, 要输入的文本
        :element_id: 输入框网页元素id

        """
        input_el = driver.find_element_by_id(element_id)
        input_el.clear()
        input_el.send_keys(text)
        # time.sleep(3.5)

    def click_by_id(self, driver, element_id = "popup-submit"):
        """点击查询按钮

        :element_id: 查询按钮网页元素id

        """
        search_el = driver.find_element_by_id(element_id)
        search_el.click()

    def get_randAccuPos(self, lis, pos_list):
        gap = 1  # random.randint(2, 3)
        # slice = sorted(random.sample(range(len(lis) - 1), gap))
        slice = [len(lis) - 2]
        self.logger.debug(len(lis))
        # print("slice is " + str(slice))
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
        # print("pos_list is " + str(pos_list))
        return pos_list

    def get_fail_signal(self, driver):  # gt_info_type
        alarm_info = driver.find_element_by_xpath("//div[@class='gt_info_text']/span").text
        self.logger.debug(alarm_info)
        print(alarm_info)
        if alarm_info.find("验证通过") != -1:
            return 0
        elif alarm_info.find("出现错误") != -1:
            return 1
        else:
            return 2

    def isElementExist(self, driver, element):
        flag = True
        try:
            driver.find_element_by_css_selector(element)
            return flag
        except:
            flag = False
            return flag

    def findstrinfile(self, filename, lookup):
        if not os.path.exists(filename):
            os.system(r'touch %s' % filename)
        with open(filename, 'r') as handle:
            for ln in handle:
                if lookup in ln:
                    return True
            else:
                return False

    def operate_trail(self, driver, data):
        print("正在查询:" + data)
        self.logger.debug("正在查询:" + data)
        image1 = self.get_image(driver, "//div[@class='gt_cut_bg gt_show']/div")
        image2 = self.get_image(driver, "//div[@class='gt_cut_fullbg gt_show']/div")
        # 计算缺口位置
        loc = self.get_diff_location(image1, image2)
        # 生成x的移动轨迹点
        track_list = self.get_track(loc)
        # 找到滑动的圆球
        element = driver.find_element_by_xpath("//div[@class='gt_slider_knob gt_show']")
        location = element.location
        # 获得滑动圆球的高度
        y = location['y']
        # 鼠标点击元素并按住不放
        # print("第一步,点击元素")
        ActionChains(driver).click_and_hold(on_element = element).perform()
        time.sleep(0.15)
        # print("第二步，拖动元素")
        pos_list = []
        pos_list = self.get_randAccuPos(track_list, pos_list)
        for p in pos_list:
            ActionChains(driver).move_to_element_with_offset(to_element = element,
                                                             xoffset = int(p) + 22,
                                                             yoffset = y - 445).perform()
            time.sleep(random.randint(10, 25) / 100)

        # xoffset=21，本质就是向后退一格。这里退了5格是因为圆球的位置和滑动条的左边缘有5格的距离
        total = -5
        movesum = 0
        step = random.randint(2, 4)
        for i in range(step):
            j = random.uniform(-5, 5)
            if i != step - 1:
                movedis = j
                movesum += j
                ActionChains(driver).move_to_element_with_offset(to_element = element,
                                                                 xoffset = 22 + movedis,
                                                                 yoffset = y - 445).perform()
            else:
                movedis = total - movesum
                ActionChains(driver).move_to_element_with_offset(to_element = element,
                                                                 xoffset = 22 + movedis,
                                                                 yoffset = y - 445).perform()
                # print(movedis)
        time.sleep(0.1)
        # print("第三步，释放鼠标")
        # 释放鼠标
        ActionChains(driver).release(on_element = element).perform()
        WebDriverWait(driver, 30).until(
            lambda the_driver: the_driver.find_element_by_xpath(
                "//div[@class='gt_info_text']").is_displayed()
        )
        return self.get_fail_signal(driver)

    # @retry
    def main(self):
        iedriver = "C:\Program Files\Internet Explorer\IEDriverServer_x64_2.53.1.exe"
        driver = webdriver.Ie(iedriver)  # webdriver.Firefox()
        driver.get("http://www.gsxt.gov.cn/index.html")
        try:
            companylist = self.src_name
            resultname = self.dest_name
            donefilename = self.done_name
            with open(companylist, 'rb') as f:
                for line in f:
                    data = line.decode('gbk', 'ignore').strip()
                    if not self.findstrinfile(donefilename, data):
                        self.input_by_id(driver, text = data, element_id = "keyword")
                        self.click_by_id(driver, element_id = "btn_query")
                        WebDriverWait(driver, 30).until(
                            lambda the_driver: the_driver.find_element_by_xpath(
                                "//div[@class='gt_slider_knob gt_show']").is_displayed()
                        )
                        # time.sleep(3)
                        fail_signal = self.operate_trail(driver, data)
                        while 2 == fail_signal:
                            fail_signal = self.operate_trail(driver, data)
                            time.sleep(3)
                        if 0 == fail_signal:
                            time.sleep(3)
                            flag1 = self.isElementExist(driver, "div.search_result.g9")
                            if flag1:
                                str1 = data + '|' + driver.find_element_by_xpath(
                                    "//div[@class='search_result g9']").text
                            else:
                                str1 = data + '|' + driver.find_element_by_xpath("//div[@class='search_result']").text
                            self.logger.debug(str1)
                            print(str1)
                            with open(resultname, 'a') as resultfile:
                                resultfile.write(str1 + '\n')
                            with open(donefilename, 'a') as donefile:
                                donefile.write(data + '\n')
                            with open(donefilename, 'r') as donefile:
                                lines = len(donefile.readlines())
                                if lines % 100 == 0:
                                    ems = SendEmail('bumpink@126.com', '*', 'cysuncn@126.com', 'nationalIC',
                                                    str(lines) + '/967', 'smtp.126.com')
                                    ems.send_email()
                        else:
                            driver.quit()
                            self.main()
                    else:
                        pass
                        # print(data + "has queried")
        finally:
            driver.quit()

