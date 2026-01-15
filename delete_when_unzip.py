# -*- coding: utf-8 -*-
# TODO: 设法完成分段zip的解压
import os
from stream_unzip import stream_unzip
import sys
import logging
import time
import hashlib
from tqdm import tqdm

# Basic logging for auditing
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Global progress bar for bytes read from original archive (updated in read_file_by_chunk)
pbar_read = None

def shift_then_truncate(file,chunk_size=1024):
    '''将文件向头部平移chunksize，并保留未被覆盖的后半部分（相当于删除头部chunksize字节）'''
    with open(file,'rb+') as f: 
        pointer = chunk_size
        while True:
            f.seek(pointer)
            chunk = f.read(chunk_size)
            if not chunk:
                break
            new_pointer = f.tell()
            if new_pointer<=chunk_size: # 文件小于chunksize，不需要向头部移动
                break
            f.seek(pointer-chunk_size)
            f.write(chunk)  # 将第k段写入k-1段的空间内
            pointer = new_pointer   # 指向第k段末尾
            # print('shift once') # debug
        f.seek(pointer-chunk_size)  # 指向平移后的末尾
        f.truncate()

def read_file_by_chunk(file,chunk_size=1024):
    '''按块读取文件，可指定块大小'''
    global pbar_read
    while True:
        with open(file,'rb') as f: 
            f.seek(0)
            chunk = f.read(chunk_size)
            pointer = f.tell()
            if not chunk:
                return
            # Update read-progress (if set) before yielding chunk
            if pbar_read is not None:
                try:
                    pbar_read.update(len(chunk))
                except Exception:
                    pass
            yield chunk
        shift_then_truncate(file,chunk_size)# [chunk_size:-1]的文件内容逐次向头部移动，相当于删除头部chunksize字节
        
def main_unzip(file,chunk_size=1024,password=None):
    '''在本地流式解压文件，边解压边删除. '''
    chunk_size = int(chunk_size)    # python IO函数只支持int值参数

    # Setup read progress bar for whole archive if size available
    global pbar_read
    try:
        total_size = os.path.getsize(file)
    except Exception:
        total_size = None
    if total_size is not None:
        pbar_read = tqdm(total=total_size, unit='B', unit_scale=True, desc='Reading archive', leave=True)
    else:
        pbar_read = None

    start_time = time.time()
    extracted_files = 0
    total_extracted_bytes = 0

    file_chunks = read_file_by_chunk(file,chunk_size)
    file_oripath,basename = os.path.split(file)
    file_folder = os.path.splitext(basename)[0]
    if not os.path.exists(os.path.join(file_oripath,file_folder)):
        os.makedirs(os.path.join(file_oripath,file_folder))
    i = 0
    # if password is not None:
    #     password = password.encode()    # 必须是二进制字符串
    try:
        for file_path_name, file_size, unzipped_chunks in stream_unzip(file_chunks,password=password,chunk_size=chunk_size):
            # print('Processing chunk {}'.format(i))  # debug
            i+=1
            rel_name = file_path_name.decode('GBK')
            file_path_name = os.path.join(file_oripath,file_folder, rel_name)
            dir_path,file_name = os.path.split(file_path_name)  # 检查文件存放路径的健全性
            # print(dir_path,file_name) # debug
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            if file_name != "":
                # Create a per-file progress bar if file_size is known
                file_pbar = None
                try:
                    size_int = int(file_size) if file_size is not None else None
                except Exception:
                    size_int = None
                if size_int and size_int > 0:
                    file_pbar = tqdm(total=size_int, unit='B', unit_scale=True, desc=f'Extracting {file_name}', leave=False)
                md5 = hashlib.md5()
                written = 0
                with open(file_path_name,'wb+') as f1:
                    for chunk in unzipped_chunks:
                        f1.write(chunk)
                        written += len(chunk)
                        total_extracted_bytes += len(chunk)
                        md5.update(chunk)
                        if file_pbar is not None:
                            file_pbar.update(len(chunk))
                    f1.flush()
                if file_pbar is not None:
                    file_pbar.close()
                extracted_files += 1
                logger.info("Extracted: %s (%s bytes) md5=%s", file_path_name, written, md5.hexdigest())
    finally:
        # cleanup/read pbar
        if pbar_read is not None:
            pbar_read.close()

    # remove original file (keep old behavior)
    try:
        os.remove(file)
        logger.info("Removed original archive: %s", file)
    except Exception as e:
        logger.warning("Could not remove original archive %s: %s", file, e)

    elapsed = time.time() - start_time
    logger.info("Finished extraction. Files: %d, Extracted bytes: %d, Time: %.2fs", extracted_files, total_extracted_bytes, elapsed)
    
if __name__ == '__main__':
    if len(sys.argv) <= 1 or len(sys.argv) >4:
        raise AttributeError('Wrong input param')
    password = None
    if len(sys.argv) > 1:
        FILE_PATH = sys.argv[1]
        CHUNK_SIZE = 1024*1024*512.0  # 512MB per chunk
    if len(sys.argv) > 2:
        FILE_PATH = sys.argv[1]
        CHUNK_SIZE = eval(sys.argv[2])
    if len(sys.argv) > 3:
        FILE_PATH = sys.argv[1]
        CHUNK_SIZE = eval(sys.argv[2])
        password = sys.argv[3]
    main_unzip(FILE_PATH,CHUNK_SIZE,password)

    # from zipfile import ZipFile

    # ZipFile.extract()