import os
import shutil
import math
import pandas as pd
import datetime as dt

#run this after the Axioma constrains are updated
#test_date=dt.datetime.now()
test_date=dt.datetime(2019,10,10)
date_path=str(test_date.year)+'/'+str(test_date.month).zfill(2)+'/'+str(test_date.date()).replace('-','')+'/'
TEST_PATH = '/home/jzhang/shared/axioma_api_wsp/'+date_path
PRODUCTION_PATH='/opt/production/'+date_path
#the production workspace name and
PRODUCTION_ws_path=TEST_PATH+'axioma_master/production_master_main_only.wsp'
PRODUCTION_ws_path_withdate=TEST_PATH+'axioma_master/production_master_main_only_%s.wsp'%str(test_date.date()).replace('-','')


def prepare_workspace_with_date(PRODUCTION_ws_path,PRODUCTION_ws_path_withdate):
    raw = file(PRODUCTION_ws_path, 'r').read()
    output_path='/user/jzhang/axioma_api_wsp/'+date_path
    raw = raw.replace('/production/current/', output_path)
    raw = raw.replace('\production\current', output_path.format(os.getcwd().replace("/", "\""))[:-1])
    fh = file(PRODUCTION_ws_path_withdate, 'w')
    fh.write(raw)
    fh.close()
    cmd='chmod -R 777 '+PRODUCTION_ws_path_withdate
    os.system(cmd)
    return



if __name__ == '__main__':
    # copy the production folder to cost test folder
    subfolder_list = ['axioma_master', 'holdings', 'optimizer']
    for subfolder in subfolder_list:
        src = PRODUCTION_PATH + subfolder
        dst = TEST_PATH
        cmd = 'cp -R ' + src + ' ' + dst
        os.system(cmd)
        cmd = 'chmod -R 777 ' + dst
        os.system(cmd)
        cmd = 'chown -R ankur:nipun ' + dst
        os.system(cmd)

    prepare_workspace_with_date(PRODUCTION_ws_path, PRODUCTION_ws_path_withdate)