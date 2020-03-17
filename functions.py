import re
import urllib.parse as urlparse
import requests
import cchardet
from pymongo import MongoClient
import config
import traceback
import json
import redis
import aiohttp


async def fetch(session, para, headers=None, timeout=40, binary=False):
    _headers = {
        'User-Agent': "haodf_app/1.0",
        "Content-Type": "application/x-www-form-urlencoded",
        "Host": "mobile-api.haodf.com",
    }
    data = {
        "pageSize": "10",
        "api": "1.2",
        "pageId": para["page"],
        "facultyId": para["faculty"],
        "hospitalFacultyId": "",
        "provinceArr": "全国,",
    }
    url = "http://mobile-api.haodf.com/patientapi/hospital_getDoctorListByFaculty"
    _headers = headers if headers else _headers
    try:
        async with session.post(url, headers=_headers, data=data, timeout=timeout, proxy="", verify_ssl=False) as response:
            status = response.status
            print(status, para)
            html = await response.json(content_type='text/html', encoding='utf-8')
    except Exception as e:
        msg = 'Failed download: {} | exception: {}, {}'.format(url, str(type(e)), str(e))
        print(msg)
        html = ''
        status = 0
    return status, html


def init_file_logger(fname):
    # config logging
    import logging
    from logging.handlers import TimedRotatingFileHandler
    ch = TimedRotatingFileHandler(fname, when="midnight")
    ch.setLevel(logging.INFO)
    # create formatter
    fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)
    # add formatter to ch
    ch.setFormatter(formatter)
    logger = logging.getLogger(fname)
    # add ch to logger
    logger.addHandler(ch)
    return logger


if __name__ == '__main__':
    # insert_faculty()
    # doctor_para()
    client = MongoClient(config.MONGO_URI)
    for item in client["haodaifu"]["doctors"].find():
        client["haodaifu"]["doctor"].insert_many(item["content"]["doctorList"])