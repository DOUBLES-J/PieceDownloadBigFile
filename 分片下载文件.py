# -*- coding=utf-8 -*-
import requests
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import os
import sys
import time


class PiecesDownLoadFile():
    def __init__(self, file_name, url, max_workers):
        self.file_name = file_name
        self.url = url
        self.max_workers = max_workers
        self.total_pieces_nums = 0
        self.part_size = 102400
        self.now_path = os.getcwd()
        self.save_file_path = os.path.join(self.now_path, self.file_name)
        print("文件存储目录：", self.save_file_path)
        if not os.path.exists(self.save_file_path):
            os.mkdir(self.save_file_path)
        # 创建线程池
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.tasks = []

    def start_thread(self, start, end, index):
        self.tasks.append(self.executor.submit(self.download_piece, start, end, index))

    def wait_thread_over(self):
        wait(self.tasks, return_when=ALL_COMPLETED)

    def download_piece(self, start, end, i):
        # 下载分片，存储数据
        headers = {'Range': 'bytes=%d-%d' % (start, end)}
        r = requests.get(self.url, headers=headers, stream=True, timeout=20)
        file_path = os.path.join(self.save_file_path, self.file_name + str(i) + ".zip")
        with open(file_path, 'wb') as fp:
            fp.write(r.content)
            fp.flush()
        file_nums = len(os.listdir(self.save_file_path))
        self.progress(file_nums, self.total_pieces_nums)

    def get_file_size(self):
        file_size = 0
        r = requests.head(self.url)
        try:
            file_size = int(r.headers['content-length'])
            return file_size
            # Content-Length获得文件主体的大小，当http服务器使用Connection:keep-alive时，不支持Content-Length
        except:
            return file_size

    def download(self, file_size):
        # 启动多线程写文件
        print("开始下载")
        for i in range(self.total_pieces_nums):
            start = self.part_size * i
            if i == self.total_pieces_nums - 1:
                end = file_size
            else:
                end = start + self.part_size - 1
            self.start_thread(start, end, i)
        time.sleep(10)
        self.wait_thread_over()
        print('\n%s 分片下载完成' % self.file_name)

    def get_no_download_part(self):
        no_parts = []
        files_list = os.listdir(self.save_file_path)
        files_index = [int(i[len(self.file_name):-4]) for i in files_list]
        for i in range(self.total_pieces_nums):
            if i not in files_index:
                no_parts.append(i)
        return no_parts

    def get_error_download_part(self):
        error_parts = []
        files_list = os.listdir(self.save_file_path)
        for f in files_list:
            if os.path.getsize(os.path.join(self.save_file_path, f)) == 0:
                error_parts.append(int(f[len(self.file_name):-4]))
        return error_parts

    def check(self):
        print("检查下载的分片：")
        # 检查所有分片是否都已正确下载
        # 检查是否有漏采的片段
        while True:
            no_parts = self.get_no_download_part()
            error_parts = self.get_error_download_part()
            error_parts.extend(no_parts)
            if not error_parts:
                break
            for index in error_parts:
                self.start_thread(self.part_size * index, self.part_size * (index + 1) - 1, index)
            self.wait_thread_over()

    def merge(self):
        print("\n开始合并分片文件：")
        # 合并下载的分片文件
        file_list = os.listdir(self.save_file_path)
        file_list.sort(key=lambda x: int(x[len(self.file_name):-4]))
        file_nums = len(file_list)
        with open(os.path.join(self.save_file_path, self.file_name), "wb") as wf:
            for index, f in enumerate(file_list):
                new_file_path = os.path.join(self.save_file_path, f)
                with open(new_file_path, "rb") as rf:
                    content = rf.read()
                    wf.write(content)
                self.progress(index + 1, file_nums)

    def progress(self, i, total_nums):
        sys.stdout.write("\r---- %10d %3.2f%%" % (i, i / total_nums * 100))
        sys.stdout.flush()

    def main(self):
        # 获取文件大小
        file_size = self.get_file_size()
        if file_size <= 0:
            print("检查URL，或不支持对线程下载")
            return
        print("文件大小（B）：", file_size)
        # 启动分片下载
        self.total_pieces_nums = file_size // self.part_size if file_size > self.part_size else 1
        print("总片数：%d" % self.total_pieces_nums)
        self.download(file_size)
        # 检查错误片
        self.check()
        # 合并分片文件
        self.merge()

        print("\n文件下载完成。")


if __name__ == '__main__':
    try:
        max_worker = int(sys.argv[1])
    except Exception as e:
        max_worker = 200
    print(max_worker)
    url = input("下载地址：").strip()
    file_name = input("保存文件名：").strip()
    download = PiecesDownLoadFile(file_name, url, max_worker)
    download.main()
    # download.merge()
