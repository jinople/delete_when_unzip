# -*- coding: utf-8 -*-
import os
from stream_unzip import stream_unzip
import sys
import re
from robust_split import robust_basename_split
import logging
import time
import hashlib
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Global progress bar for bytes read from original volumes
pbar_read = None

def read_file_by_chunk(file_basepath,chunk_size=1024):
    '''按块读取文件，可指定块大小'''
    global pbar_read
    file_path,file_basename_zip = os.path.split(file_basepath)
    if file_path == '':
        file_path = './'
    file_basename = robust_basename_split(file_basename_zip)
    file_list = []
    files = os.listdir(file_path)
    # 筛出file_basename.zip, file_basename.z01, file_basename.z02 ...
    pattern1 = re.compile(rf"{re.escape(file_basename)}\.z\d+",re.I)
    pattern2 = re.compile(rf"{re.escape(file_basename)}\.zip",re.I)
    pattern3 = re.compile(rf"{re.escape(file_basename)}\.zip\.\d+",re.I)
    for file in files:
        if pattern1.match(file) or pattern2.match(file) or pattern3.match(file):
        # if file.startswith(file_basename) and os.path.isfile(os.path.join(file_path, file)):
            file_list.append(os.path.join(file_path, file)) # 将按照z01,z02,...zip顺序排列
    # print(file_list)    # debug
    for file in file_list:
        with open(file,'rb') as f: 
            _,file_ext = os.path.splitext(file) 
            if file_ext == '.z01' or file_ext == '.Z01':
                f.seek(4)   # .z01文件需要跳过头部4字节
            while True: 
                chunk = f.read(chunk_size)
                pointer = f.tell()
                if not chunk:
                    break
                # Update read-progress (if set)
                if pbar_read is not None:
                    try:
                        pbar_read.update(len(chunk))
                    except Exception:
                        pass
                yield chunk
        remove_one_chunk(file)  # 完成一个分段文件的解压，删除该块压缩文件
    return

def remove_one_chunk(file):
    '''删除已解压的分段压缩文件'''
    try:
        os.remove(file)
        logger.info("Removed volume: %s", file)
    except Exception as e:
        logger.warning("Could not remove %s: %s", file, e)

def main_unzip(file,chunk_size=1024,password=None):
    '''在本地流式解压文件，边解压边删除。'''
    chunk_size = int(chunk_size)    # python IO函数只支持int值参数

    # Attempt to set a read progress bar across all volumes if total can be estimated
    global pbar_read
    try:
        file_path, _ = os.path.split(file)
        if file_path == '':
            file_path = './'
        # total size of matched volumes (best-effort)
        base = robust_basename_split(os.path.basename(file))
        total = 0
        for f in os.listdir(file_path):
            if f.startswith(base):
                try:
                    total += os.path.getsize(os.path.join(file_path, f))
                except Exception:
                    pass
        if total > 0:
            pbar_read = tqdm(total=total, unit='B', unit_scale=True, desc='Reading volumes', leave=True)
        else:
            pbar_read = None
    except Exception:
        pbar_read = None

    start_time = time.time()
    extracted_files = 0
    total_extracted_bytes = 0

    file_chunks = read_file_by_chunk(file,chunk_size)
    file_oripath,basename = os.path.split(file)
    file_folder = robust_basename_split(basename)
    # if file_folder.endswith('.zip') or file_folder.endswith('.ZIP'):    # 针对.zip.00x多重分段文件
    #     file_folder,_ = os.path.splitext(file_folder)
    if not os.path.exists(os.path.join(file_oripath,file_folder)):
        os.makedirs(os.path.join(file_oripath,file_folder))
    i = 0
    # if password is not None:
    #     password = password.encode()    # 必须是二进制字符串
    try:
        for file_path_name, file_size, unzipped_chunks in stream_unzip(file_chunks,password=password):
            # print('Processing chunk {}'.format(i))  # debug
            i+=1
            rel_name = file_path_name.decode('GBK')
            file_path_name = os.path.join(file_oripath,file_folder, rel_name)
            dir_path,file_name = os.path.split(file_path_name)  # 检查文件存放路径的健全性
            # print(dir_path,file_name) # debug
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            if file_name != "":
                md5 = hashlib.md5()
                written = 0
                # per-file progress (if size known)
                file_pbar = None
                try:
                    size_int = int(file_size) if file_size is not None else None
                except Exception:
                    size_int = None
                if size_int and size_int > 0:
                    file_pbar = tqdm(total=size_int, unit='B', unit_scale=True, desc=f'Extracting {file_name}', leave=False)
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
        if pbar_read is not None:
            pbar_read.close()

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
