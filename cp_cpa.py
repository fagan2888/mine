import pandas as pd
import datetime
import os
import shutil

if __name__ == '__main__':
    pass
    signals = ['b2proe_in', 'cf12p_in', 'cratio_e', 'd12p_in', 'd12p_tsv_in', 'e12p_in', 'e12p_tsv_in', 's12ev_tsv_in', 'updn_e2_in', 'updn2midc_in']
    signals = [ 'd12p_tsv_in', 'e12p_tsv_in',  's12ev_tsv_in', 'cf12p_in', 'd12p_in', 'e12p_in']
    signals = ['b2proe_in']
    signals = ['cratio_e']
    signals = ['updn_e2_in', 'updn2midc_in']
    signals = ['fe12p', 'fe12p_in', 'it_fe12p']
    signals = ['fe12p_in']
    signals = ['drec']
    startdt = datetime.datetime(2018,10,7)
    enddt = datetime.datetime(2019,10,14)
    cpa_root = '/local/res-fs/sandbox/jzhang/signals/'
    cpa_root = '/research/cpa/jzhang/signals/'
    root_mine_signal = '/home/jzhang/'
    root_prod = '/production/'
    num_sigs = len(signals)
    my_alpha_folder = '/alphas/'
    # my_alpha_folder = '/alphas_prod_consensus/'
    for i in range(num_sigs):
        s = signals[i]
        s_before = s + '_r'
        target_dir_before = cpa_root + s_before + '_before/' + s_before + '/'

        _p = cpa_root + s_before + '_before/'
        if not os.path.exists(_p):
            os.mkdir(_p)
        if not os.path.exists(target_dir_before):
            os.mkdir(target_dir_before)

        target_dir_after = cpa_root + s + '_after/' + s + '/'
        _p = cpa_root + s + '_after/'
        if not os.path.exists(_p):
            os.mkdir(_p)
        if not os.path.exists(target_dir_after):
            os.mkdir(target_dir_after)

        #now loop thru the days to copy the lpha files
        _dates = pd.DateRange(startdt, enddt, offset=pd.DateOffset())
        for _d in _dates:
            print('copying for signal and date:', str(i+1), str(num_sigs), str(_d))
            _d_prod = _d + datetime.timedelta(1)
            _y = str(_d.year)
            _m = str(_d.month).zfill(2)

            _y_prod = str(_d_prod.year)
            _m_prod = str(_d_prod.month).zfill(2)


            ymd = _d.strftime("%Y%m%d")
            ymd_prod = _d_prod.strftime("%Y%m%d")

            _fn = s_before + '_' + ymd + '.alp'
            src_prod = root_prod + _y_prod + '/' + _m_prod + '/' + ymd_prod + '/alphagen/npxchnpak/alpha_v5/' + s_before + '/' + _fn
            shutil.copyfile(src_prod, target_dir_before + _fn)

            _fn_after = s + '_' + ymd + '.alp'
            src_after = root_mine_signal + _y + '/' + _m + '/' + ymd + my_alpha_folder + s + '/' + _fn_after
            shutil.copyfile(src_after, target_dir_after + _fn_after)

