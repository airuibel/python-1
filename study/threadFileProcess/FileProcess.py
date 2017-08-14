# -*- coding : utf-8 -*-

class FileProcess(object):
    def __init__(self, src_filename, clean_filename, des_dir):
        self.des_dir = des_dir
        self.src_filename = src_filename
        self.clean_filename = clean_filename

    @staticmethod
    def isZh(s):
        if len(s) < 5:
            return False
        if not ('\u4e00' <= s[0] <= '\u9fa5'):
            return False
        return True

    def get_clean_file(self, splitop, field, flag):
        try:
            clean_file = open(self.clean_filename, 'w')
            with open(self.src_filename, mode = 'rb') as reader:
                for line in reader:
                    data = line.decode('gbk', 'ignore').strip().split(splitop)
                    if flag:
                        if len(data) >= 1:
                            word = data[field]
                            if self.isZh(word):
                                clean_file.write("|".join(data) + '\n')
                            else:
                                pass
                    else:
                        clean_file.write(line.decode('gbk', 'ignore').strip() + '\n')
        finally:
            clean_file.close()

    def split_file(self, lines):
        file_list = []
        with open(self.clean_filename, mode = 'rb') as reader:
            counter = 0
            findex = 0
            for line in reader:
                data = line.decode('gbk', 'ignore').strip()
                if counter == 0:
                    writer = open(self.src_filename + '_' + str(findex), 'w')
                writer.write(data + '\n')
                counter += 1
                # print(counter)
                if counter >= lines:
                    writer.close()
                    file_list.append(self.src_filename + '_' + str(findex))
                    counter = 0
                    findex += 1
        return file_list
